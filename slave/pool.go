package slave

import (
	"github.com/itgeniusshuai/mysql_slave/tools"
	"time"
	"fmt"
	"runtime"
	"errors"
	"net"
)

type Pool struct {
	// 连接
	Conns []*MysqlConnection
	// 池大小
	PoolSize uint8
	// 主机
	Host string
	// 端口
	Port int
	// 用户
	User string
	// 密码
	Pwd string
	// 主机id
	ServerId uint32
	// 事件处理函数
	DealFunc func(eventStruct BinlogEventStruct)
	// 上次处理事件时间戳
	LastEventId int64

}

// 获取连接池
func MakePool(poolSize uint8,host string,port int,user string,pwd string,serverId uint32)Pool{
	if poolSize == 0 {
		poolSize = DEFAULT_POOL_SIZE
	}
	tools.Println("make conn pool size is %d",poolSize)
	var pool = Pool{PoolSize:poolSize,Host:host,Port:port,User:user,Pwd:pwd,ServerId:serverId}
	var i uint8 = 0
	for ;i < poolSize;i++{
		conn := GetMysqlConnection(host,port,user,pwd,serverId)
		pool.Conns = append(pool.Conns, conn)
	}
	return pool
}

// 监听binlog
func (this *Pool)ListenBinlogAndParse( dealEvent func(v BinlogEventStruct)){
	this.DealFunc = dealEvent
	// 过滤重复事件
	var dealPoolEvent = func(v BinlogEventStruct){
		currEventId := v.BinlogHeader.Id
		if currEventId > this.LastEventId{
			this.LastEventId = currEventId
			dealEvent(v)
		}else{
			tools.Println("event has been dealed by other conn")
		}
	}
	for _,conn := range this.Conns{
		conn.StartBinlogDumpAndListen(dealPoolEvent)
	}
	// 开启池连接检测
	go this.CheckPoolConn()
}

// 检测池连接并断了重新连接
func (this *Pool)CheckPoolConn(){
	//tools.Println("start check pool conn every second")
	tick := time.NewTicker(1*time.Second)
	for _ = range tick.C {
		//tools.Println("checking pool conn every second")
		err := this.check()
		if err != nil{
			//fmt.Println("check pool error ",err.Error())
		}
	}
}

func (this *Pool)check() (err error){
	defer func() {
		//fmt.Println("check after error")
		if r := recover(); r != nil {
			err = r.(error)
			if r, ok := r.(runtime.Error); ok {
				err = errors.New(r.Error())
			}
			if s, ok := r.(string); ok {
				err =  errors.New(s)
			}
		}
		err =  errors.New("check error")
	}()
	var now= time.Now().Second()
	//fmt.Println(fmt.Sprintf("this conns [%v]",this.Conns))
	for i, conn := range this.Conns {
		//fmt.Println(fmt.Sprintf("this conns [%v]",conn))
		//tools.Println("check pool conn every conn one")
		if conn == nil || conn.LastReceivedTime == nil || conn.LastReceivedTime.Second()+10 < now {
			//tools.Println("check pool could be unused")
			if conn != nil{
				var bs = []byte{1}
				//tools.Println("write a byte on this conn")
				err := write(conn.Conn,bs)
				if err == nil{
					// if can write conn can use
					//tools.Println("conn can use")
					continue
				}
			}
			//tools.Println("conn"+conn.id+" has interrupt")
			if conn != nil{
				conn.Close()
			}
			conn = GetMysqlConnection(this.Host, this.Port, this.User, this.Pwd, this.ServerId)
			var now = time.Now()
			conn.LastReceivedTime = &now
			this.Conns[i] = conn
			tools.Println("reconnect to  mysql")
			conn.StartBinlogDumpAndListen(this.DealFunc)
		}
	}
	return nil
}

func write(conn net.Conn,bs []byte)(err error){
	defer func() {
		//fmt.Println("after write err")
		if r := recover(); r != nil {
			err = r.(error)
			if r, ok := r.(runtime.Error); ok {
				err = errors.New(r.Error())
			}
			if s, ok := r.(string); ok {
				err =  errors.New(s)
			}
		}
		err = errors.New("write failed")
	}()
	_,err = conn.Write(bs)
	return err;

}

