package main

import (
	"github.com/itgeniusshuai/mysql_slave/slave"
	"fmt"
	"github.com/itgeniusshuai/mysql_slave/tools"
	"github.com/kataras/iris"
	"github.com/itgeniusshuai/mysql_slave/controllers"
)

var Semaphore = make(chan int,1)

func main(){
	//mainFunc("127.0.0.1",3306,"root","",1)

	// web
	app := iris.New()
	app.RegisterView(iris.HTML("./static/views", ".html"))
	app.StaticWeb("/static/js","./static/js")

	// 首页
	app.Get("/", controllers.Index)

	app.Run(iris.Addr(":8080"))
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

