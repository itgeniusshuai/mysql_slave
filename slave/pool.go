package slave

import (
	"github.com/itgeniusshuai/mysql_slave/tools"
	"time"
	"github.com/golang/lint/testdata"
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
	for _,conn := range this.Conns{
		conn.StartBinlogDumpAndListen(dealEvent)
	}
	// 开启池连接检测
	go this.CheckPoolConn()
}

func (this *Pool)CheckPoolConn(){
	tools.Println("check pool conn every second")
	tick := time.NewTicker(1*time.Second)
	for _ = range tick.C {
		var now= time.Now().Nanosecond()
		for i, conn := range this.Conns {
			if conn.LastReceivedTime.Nanosecond()+10 < now {
				conn = GetMysqlConnection(this.Host, this.Port, this.User, this.Pwd, this.ServerId)
				this.Conns[i] = conn
				conn.StartBinlogDumpAndListen(this.DealFunc)
			}
		}
	}
}