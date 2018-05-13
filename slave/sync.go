package slave

var BinlogChan = make(chan BinlogEventStruct,4*1024)
