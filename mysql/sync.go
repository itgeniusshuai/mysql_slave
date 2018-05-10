package mysql

import (
	"net"
	"github.com/itgeniusshuai/go_common/common"
	"errors"
	"bytes"
	"fmt"
	"crypto/sha1"
	"encoding/binary"
)

type MysqlConnection struct{
	conn net.Conn
}

var msgseq byte = 0x00

func ConnectMysql(host string, port int, user string, pwd string){
	conn, err := net.Dial("tcp",host+":"+common.IntToStr(port))
	if err != nil{
		panic(errors.New(err.Error()))
	}
	bs := make([]byte,1024)
	n,err := conn.Read(bs)
	if err != nil{
		panic(errors.New(err.Error()))
	}
	bs=bs[:n]

	var clientPacket = GetWriteAuthShackPacket(bs,user,pwd)
	fmt.Println(clientPacket)
	conn.Write(clientPacket)
	_,err = conn.Read(bs)
	if err != nil{
		panic(errors.New(err.Error()))
	}
	fmt.Println(common.BytesToStr(bs))
	conn.Close()
}

func ParseInitShackPacket(data []byte) ProtocolPacket{
	packet := ProtocolPacket{}
	packet.PacketLen = common.BytesToIntWithMin(data[:3])
	packet.PacketNum = data[3]
	msgseq = data[3]
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

func GetWriteAuthShackPacket(serverPacketBytes []byte,user string, pwd string) []byte{
	protocolPacket := ParseInitShackPacket(serverPacketBytes)
	packet := protocolPacket.Packet
	var shackServerPacket = packet.(ServerShackPacket)
	fmt.Println(uint32(binary.LittleEndian.Uint16(shackServerPacket.PowerFlagLow)))
	powerFlag :=
		CLIENT_PROTOCOL_41 |
		CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD |
		CLIENT_TRANSACTIONS |
		CLIENT_PLUGIN_AUTH |
		CLIENT_LOCAL_FILES |
		CLIENT_MULTI_RESULTS|
		uint32(binary.LittleEndian.Uint16(shackServerPacket.PowerFlagLow))&CLIENT_LONG_FLAG


	fmt.Println(powerFlag)
	challengeRandNum := shackServerPacket.ChallengeRandNum
	challengeRandNum = append(challengeRandNum, shackServerPacket.ChallengeRandNum2...)

	scrambleBuff := scramblePassword(challengeRandNum,common.StrToBytes(pwd))
	// 报文头4+权能4+最大信息长度4+字符编码1+填充值23+用户名n+挑战认证数据n+【数据库n】
	pktLen := 4 + 4 + 4 + 1 + 23 + len(user) + 1 + 1 + len(scrambleBuff) + 21 + 1
	var data = make([]byte,pktLen)
	copy(data,serverPacketBytes[:pktLen])
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
	pos += copy(data[pos:],user)
	data[pos] = 0
	pos ++
	data[pos] = byte(len(scrambleBuff))
	pos += 1 + copy(data[pos+1:], scrambleBuff)
	pos += copy(data[pos:], "mysql_native_password")
	data[pos] = 0x00
	msgseq++
	data[3] = msgseq
	pktLen = pktLen - 4
	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	return data

}

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
