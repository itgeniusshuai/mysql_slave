package slave

var BinlogChan = make(chan BinlogEvent,4*1024)
