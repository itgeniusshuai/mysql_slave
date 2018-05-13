package slave

import (
	"github.com/itgeniusshuai/go_common/common"
)

// 事件体
type BinlogEvent interface {
	ParseEvent(bs []byte)
}

// 包含头和体
type BinlogEventStruct struct {
	BinlogHeader BinlogHeader
	BinlogEvent BinlogEvent
}

// 行事件
type RowBinlogEvent struct {
	DbName string
	TableName string
	ColumnNames []interface{}
	Pos uint
	RowDatas [][]interface{}
}

// 事件头
type BinlogHeader struct{
	TimeStamp int
	TypeCode byte
	ServerId []byte
	EventLength int
	NextPosition int
	Flags []byte
	ExtraHeaders []byte
}

// 解析事件头每个事件都一样
func ParseBinlogHeader(bs []byte) *BinlogHeader{
	binlogHeader := BinlogHeader{}
	binlogHeader.TimeStamp = common.BytesToIntWithMin(bs[:4])
	binlogHeader.TypeCode = bs[4]
	binlogHeader.ServerId = bs[5:9]
	binlogHeader.EventLength = common.BytesToIntWithMin(bs[9:13])
	binlogHeader.NextPosition = common.BytesToIntWithMin(bs[13:17])
	binlogHeader.Flags = bs[17:19]
	return &binlogHeader
}

// 解析事件（包含头和体）
func ParseEvent(bs []byte) *BinlogEventStruct{
	binlogEventStruct := BinlogEventStruct{}
	header := ParseBinlogHeader(bs)
	binlogEventStruct.BinlogHeader = *header
	var binlogEvent BinlogEvent
	// 只解析增删改查的行事件
	switch header.TypeCode {
	case WRITE_ROWS_EVENT,WRITE_ROWS_EVENT_V1,UPDATE_ROWS_EVENT,UPDATE_ROWS_EVENT_V1,DELETE_ROWS_EVENT,DELETE_ROWS_EVENT_V1:
		binlogEvent = &RowBinlogEvent{}
	default:
		return nil
	}
	binlogEvent.ParseEvent(bs)
	binlogEventStruct.BinlogEvent = binlogEvent
	return &binlogEventStruct
}

// 具体事件的解析实现
func (this *RowBinlogEvent) ParseEvent(bs []byte){

}
