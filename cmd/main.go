package main

import (
	"github.com/itgeniusshuai/mysql_slave/slave"
	"fmt"
	"time"
	"github.com/itgeniusshuai/mysql_slave/tools"
)

func main(){
	//var tbs = []byte{83,0,0,0,133,162,10,0,0,0,0,0,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,100,98,49,0,20,58,241,51,154,41,237,164,253,212,137,103,32,207,79,57,236,149,216,150,111,109,121,115,113,108,95,110,97,116,105,118,101,95,112,97,115,115,119,111,114,100,0}
	//fmt.Println(len(tbs))
	conn := slave.GetMysqlConnection("172.16.98.17",3306,"db1","EEB9703F3F",9812)
	//conn := slave.GetMysqlConnection("localhost",3306,"root","3108220w",1)
	if conn != nil{
		e := conn.StartBinlogDumpAndListen(func(event slave.BinlogEventStruct){
			tools.Println("binlog event content is [%s]",event.BinlogHeader.TypeCode)
		})
		if e != nil{
			tools.Println("binlog dump and listen error ",e.Error())
		}
	}

	time.Sleep(time.Hour*10)
	fmt.Println(conn)
	//var bs = []byte{10,53,46,54,46,50,52,45,55,50,46,50,45,108,111,103,0,109,24,182,42,120,106,42,48,83,51,65,46,0,255,247,33,2,0,127,128,21,0,0,0,0,0,0,0,0,0,0,98,36,42,69,95,46,99,45,82,55,90,67,0,109,121,115,113,108,95,110,97,116,105,118,101,95,112,97,115,115,119,111,114,100,0}
	//var len = len(bs)
	//var bs2 = []byte{byte(len),0,0,0}
	//bs2 = append(bs2, bs...)
	//bs3 := mysql.GetWriteAuthShackPacket(bs2,"db1","EEB9703F3F")
	//fmt.Println(bs3)
}
