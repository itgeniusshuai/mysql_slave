package slave

type BinlogEvent struct {
	Content []byte
}

func ParseBinlogEvent(bs []byte) *BinlogEvent{
	return &BinlogEvent{Content:bs}
}
