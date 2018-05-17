package slave

import (
	"net"
	"github.com/itgeniusshuai/go_common/common"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"sync"
	"github.com/juju/errors"
	"github.com/itgeniusshuai/mysql_slave/tools"
	"os"
	"time"
)

type MysqlConnection struct{
	Conn net.Conn
	User string
	Pwd string
	MsgSeq byte
	Host string
	Port int
	seqLock sync.RWMutex
	ServerId uint32

	LastReceivedTime time.Time
}

var buffer []byte = make([]byte,1024)
var sizeBuffer []byte = make([]byte,3)

func GetMysqlConnection(host string, port int, user string, pwd string,serverId uint32)(*MysqlConnection){
	myConn := MysqlConnection{Host:host,User:user,Pwd:pwd,Port:port,ServerId:serverId}
	// 连接mysql
	err := myConn.ConnectMysql()
	if err != nil{
		return nil
	}
	return &myConn
}

func (this *MysqlConnection)ConnectMysql() error{
	conn, err := net.Dial("tcp",this.Host+":"+common.IntToStr(this.Port))
	tools.Println("connected to host[%s] port[%d]",this.Host,this.Port)
	if err != nil{
		tools.Println("can't connect to host[%s] port[%d]",this.Host,this.Port)
		return err
	}
	this.Conn = conn
	var clientPacket = this.GetWriteAuthShackPacket()
	this.WriteServerData(clientPacket)
	res,_ := this.ReadServerData()
	if(res[4] == 0){
		tools.Println("auth successful")
		return nil
	}
	return errors.New("auth failed")
}

// 考虑分包
// 分包格式为
// ff ff ff 00
// 00 00 00 01
// 00 00 00 02
// 直到小于0xfffff
func (this *MysqlConnection)ReadServerData()([]byte,error){
	var bs []byte
	var flag = false
	for{
		n,err := this.Conn.Read(sizeBuffer)
		if n == 0{
			return bs,nil
		}
		if err != nil{
			return nil,err
		}
		bs = append(bs, sizeBuffer...)
		pkLen := common.BytesToIntWithMin(bs)
		// 分包
		var bs2 []byte
		switch pkLen {
		case MAX_PACKET_SIZE,0x00:
			//分包内容，读取0xfffff字节
			 bs2 = make([]byte,MAX_PACKET_SIZE)
		default:
			// 不分包
			bs2 = make([]byte,pkLen+1)
			flag = true
		}
		n,_ = this.Conn.Read(bs2)
		bs = append(bs, bs2...)
		this.SetMsgSeq(bs[3]+1)
		if flag{
			break
		}
	}

	return bs,nil
}

func (this *MysqlConnection)WriteServerData(data []byte) error{
	length := len(data) - 4
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = this.GetAndAddSeqMsg()
	_,error := this.Conn.Write(data)
	return error
}

// 解析服务端
func (this *MysqlConnection)ParseInitShackPacket(initPacketContent []byte) ProtocolPacket{
	data := initPacketContent
	packet := ProtocolPacket{}
	packet.PacketLen = common.BytesToIntWithMin(data[:3])
	packet.PacketNum = data[3]
	this.MsgSeq = data[3] + 1
	handleShackServerPacket := ServerShackPacket{}
	handleShackServerPacket.ProtocolVersionNum = data[4]
	pos := bytes.IndexByte(data[5:],0x00)+5
	handleShackServerPacket.ProtocolVersion = common.BytesToStr(data[5:pos+1])
	pos = pos + 1
	handleShackServerPacket.ServerThreadId = common.BytesToIntWithMin(data[pos:pos +4])
	pos = pos + 4
	handleShackServerPacket.ChallengeRandNum = data[pos:pos+8]
	handleShackServerPacket.FullValue = 0x00
	pos = pos + 9
	handleShackServerPacket.PowerFlagLow = data[pos:pos+2]
	pos = pos + 2
	handleShackServerPacket.Charset = data[pos]
	pos = pos + 1
	handleShackServerPacket.ServerStatus = data[pos:pos + 2]
	pos = pos + 2
	handleShackServerPacket.PowerFlagHigh = data[pos:pos + 2]
	pos = pos + 2
	handleShackServerPacket.ChallengeLength = data[pos]
	pos = pos + 1
	handleShackServerPacket.FullValue1 = data[pos:pos+10]
	pos = pos + 10
	handleShackServerPacket.ChallengeRandNum2 = data[pos:pos+12]
	packet.Packet = handleShackServerPacket
	return packet
}

// 客户端解析握手
func (this *MysqlConnection)GetWriteAuthShackPacket() []byte{
	serverData,_ := this.ReadServerData()
	protocolPacket := this.ParseInitShackPacket(serverData)
	packet := protocolPacket.Packet
	var shackServerPacket = packet.(ServerShackPacket)
	//fmt.tools.Println(uint32(binary.LittleEndian.Uint16(shackServerPacket.PowerFlagLow)))
	powerFlag :=
		CLIENT_PROTOCOL_41 |
		CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD |
		CLIENT_TRANSACTIONS |
			CLIENT_LONG_FLAG
		//CLIENT_PLUGIN_AUTH |
		//CLIENT_LOCAL_FILES |
		//CLIENT_MULTI_RESULTS|
		//uint32(binary.LittleEndian.Uint16(shackServerPacket.PowerFlagLow))&CLIENT_LONG_FLAG


	//fmt.tools.Println(powerFlag)
	challengeRandNum := shackServerPacket.ChallengeRandNum
	challengeRandNum = append(challengeRandNum, shackServerPacket.ChallengeRandNum2...)

	scrambleBuff := scramblePassword(challengeRandNum,common.StrToBytes(this.Pwd))
	// 报文头4+权能4+最大信息长度4+字符编码1+填充值23+用户名n+挑战认证数据n+【数据库n】
	pktLen := 4 + 4 + 4 + 1 + 23 + len(this.User) + 1 + 1 + len(scrambleBuff) + 21 + 1
	var data = make([]byte,pktLen)
	data[4] = byte(powerFlag)
	data[5] = byte(powerFlag >> 8)
	data[6] = byte(powerFlag >> 16)
	data[7] = byte(powerFlag >> 24)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00
	data[12] = 33
	pos := 13
	for ; pos < 13+23; pos++{
		data[pos] = 0x00
	}
	pos += copy(data[pos:],this.User)
	data[pos] = 0
	pos ++
	data[pos] = byte(len(scrambleBuff))
	pos += 1 + copy(data[pos+1:], scrambleBuff)
	pos += copy(data[pos:], "mysql_native_password")
	data[pos] = 0x00
	return data

}

// 握手密码加密
func scramblePassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

func (this *MysqlConnection) StartBinlogDumpAndListen(dealBinlogFunc func(binlogEventStruct BinlogEventStruct)) error{
	tools.Println("register as a slave")
	e := this.RegisterSlave()
	if e != nil{
		return e
	}
	tools.Println("dump binlog")
	isOk := this.DumpBinlog()
	if !isOk{
		return errors.New("dump binlog failed")
	}
	// 启动日志监听
	tools.Println("listen binlog")
	go this.ListenBinlog()
	// 处理日志
	go func(){
		for {
			select {
			case v := <-BinlogChan:
				dealBinlogFunc(v)
			}
		}
	}()
	return nil
}

func (this *MysqlConnection)ListenBinlog(){
	tools.Println("begin listen binlog")
	for{
		bs,err := this.ReadServerData()
		if err != nil{
			tools.Println("read Binlog error:",err.Error())
			continue
		}
		if bs == nil{
			continue
		}
		if bs[4] != 0{
			continue
		}
		tools.Println("parse []byte to BinlogEvent")
		binlogEvent := ParseEvent(bs)
		if binlogEvent == nil{
			tools.Println("parse nothing don't send to chan")
			continue
		}
		tools.Println("send BinlogEvent to chan")
		BinlogChan <- *binlogEvent
	}
}

// 注册为备用机器
func (this *MysqlConnection)RegisterSlave() error{
	// binlog主从事件校验
	tools.Println("clear checknum")
	e := this.Execute(`SET @master_binlog_checksum='NONE'`)
	if e != nil{
		return e
	}
	// 心跳周期
	tools.Println("set heartbeat period")
	//this.Execute(`SET @master_heartbeat_period=1;`)
	if (e != nil){
		return e
	}
	// 伪装成从服务器
	tools.Println("writer register packet")
	e = this.WriteRegisterSlavePacket()
	if (e != nil){
		return e
	}
	isOk := this.ReadOkResult()
	if !isOk{
		return errors.New("register slave faild")
	}


	// 半同步复制
	//tools.Println("start semi sync")
	//this.Execute(`SET @rpl_semi_sync_slave = 1;`)
	//if (e != nil){
	//	return e
	//}
	return nil
}

func (this *MysqlConnection) DumpBinlog() bool{
	tools.Println("write binlog dump packet")
	err := this.WriteBinLogDumpPacket()
	if err != nil{
		tools.Println("write binlog packet error"+err.Error())
		return false
	}
	isOk := this.ReadOkResult()
	return isOk
}

func (this *MysqlConnection) WriteRegisterSlavePacket() error{
	hostname,_ := os.Hostname()
	user := this.User
	password := this.Pwd
	this.SetMsgSeq(0)
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(user)+1+len(password)+2+4+4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], this.ServerId)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(this.User))
	pos++
	n = copy(data[pos:], this.User)
	pos += n

	data[pos] = uint8(len(this.Pwd))
	pos++
	n = copy(data[pos:], this.Pwd)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], uint16(this.Port))
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return this.WriteServerData(data)
}

// binlog 监听
func (this *MysqlConnection)WriteBinLogDumpPacket() error{
	// 开启binlog dump,监听binlog
	this.SetMsgSeq(0)

	data := make([]byte, 4+1+4+2+4+len(""))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], 4)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NEVER_STOP)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], this.ServerId)
	pos += 4

	copy(data[pos:], "")

	return this.WriteServerData(data)
}

// 发送指令
func(this *MysqlConnection)SendCommand(command byte,arg string) error{
	this.SetMsgSeq(0)
	length := len(arg) + 1
	data := make([]byte, length+4)
	data[4] = command
	copy(data[5:], arg)
	return this.WriteServerData(data)
}

// 执行查询类指令
func (this *MysqlConnection)Execute(sql string)error{
	err := this.SendCommand(COM_QUERY,sql)
	if err != nil{
		tools.Println("execute sql [%s] error :%s",sql,err.Error())
	}
	res := this.ReadOkResult()
	if !res {
		return errors.New("execute [%s] failed")
	}
	return nil
}


func (this *MysqlConnection)GetAndAddSeqMsg()byte{
	defer this.seqLock.Unlock()
	this.seqLock.Lock()
	res := this.MsgSeq
	this.MsgSeq ++
	return res
}

func (this *MysqlConnection)SetMsgSeq(msgSeq byte){
	defer this.seqLock.Unlock()
	this.seqLock.Lock()
	this.MsgSeq = msgSeq
}

func (this *MysqlConnection)ReadOkResult() bool{
	res,err := this.ReadServerData()
	if err != nil{
		tools.Println("it is not a ok result error %s",err.Error())
		return false
	}
	if res[4] == 0{
		return true
	}
	tools.Println("it is not a ok result,result is "+common.BytesToStr(res[4:]))
	return false
}


