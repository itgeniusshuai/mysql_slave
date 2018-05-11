package mysql

var BinlogChan = make(chan interface{},4*1024)
