package main

import (
	"github.com/itgeniusshuai/mysql_slave/slave"
	"fmt"
	"github.com/itgeniusshuai/mysql_slave/tools"
)

var Semaphore = make(chan int,1)

func main(){
	//mainFunc("127.0.0.1",3306,"root","",1)
	mainFunc("172.18.98.12",3306,"db2","EEB9703F3F",1)

	// web
	//controllers.InitWeb()
	select{
	case <-Semaphore:
		fmt.Println("exit")
	}
}

func mainFunc(host string,port int,user string,pwd string,serverId uint32){
	pool := slave.MakePool(1,host,port,user,pwd,serverId)
	pool.ListenBinlogAndParse(dealBinlogEvent)
}

func dealBinlogEvent(eventStruct slave.BinlogEventStruct){
	fmt.Println("event struct",eventStruct.BinlogHeader.Id)
	switch v := eventStruct.BinlogEvent.(type) {
	case *slave.RowBinlogEvent:
		tools.Println("Rows Binlog Event Db Name [%s],Table Name [%s]",v.DbName,v.TableName)
	case *slave.TableMapBinlogEvent:
		tools.Println("Table Map Event Db Name [%s],Table Name [%s]",v.DbName,v.TableName)
	default:
		tools.Println("Other event type don't deal")
	}
}

