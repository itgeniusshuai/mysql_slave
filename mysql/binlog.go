package mysql

type BinlogEvent struct {

}

func ParseBinlogEvent(bs []byte) *BinlogEvent{
	return &BinlogEvent{}
}
