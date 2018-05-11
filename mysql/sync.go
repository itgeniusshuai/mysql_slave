package mysql

var BinlogChan = make(chan BinlogEvent,4*1024)
