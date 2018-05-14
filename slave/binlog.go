package slave

import (
	"github.com/itgeniusshuai/go_common/common"
	"bytes"
)

var tableMap = make(map[int]TableMapBinlogEvent,0)

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
	TableId int
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
	case TABLE_MAP_EVENT:
		binlogEvent = &TableMapBinlogEvent{}
	default:
		return nil
	}
	binlogEvent.ParseEvent(bs)
	binlogEventStruct.BinlogEvent = binlogEvent
	return &binlogEventStruct
}

// 具体事件的解析实现
func (this *RowBinlogEvent) ParseEvent(bs []byte){
	pos := 19
	this.TableId = common.BytesToIntWithMin(bs[pos:pos+6])
	pos += 6 + 2 // 2 bytes Reserved for future use.


}

type TableMapBinlogEvent struct {
	TableId int
	DbNameLength byte
	DbName string
	TableNameLength byte
	TableName string
	ColumnCount int
	ColumnTypes []byte
	MetaBlockLength int
	MetaBlock []byte
	FieldIsNull []byte

}

func (this *TableMapBinlogEvent)ParseEvent(bs []byte){
	pos := 19
	this.TableId = common.BytesToIntWithMin(bs[pos:pos+6])
	pos += 6 + 2 // 2 bytes Reserved for future use.
	// 存入表映射
	this.DbNameLength = bs[pos]
	pos ++
	// 数据库名称可变长度
	endPos := bytes.IndexByte(bs[pos:],0x00)+1
	this.DbName = common.BytesToStr(bs[pos:endPos])
	pos = endPos
	// 表名长度 1byte
	this.TableNameLength = bs[pos]
	// 表名 可变长度
	endPos = bytes.IndexByte(bs[pos:],0x00)+1
	this.TableName = common.BytesToStr(bs[pos:endPos])
	pos = endPos
	// 表中列数 int
	this.ColumnCount = common.BytesToIntWithMin(bs[pos:pos + 4])
	pos += 4
	// 列类型 可变长度，每列一个字节
	this.ColumnTypes = bs[pos: pos+this.ColumnCount]
	pos += pos+this.ColumnCount
	// 元数据块长度 int
	this.MetaBlockLength = common.BytesToIntWithMin(bs[pos:pos + 4])
	// 元数据块 可变长度
	// 是否每列为空 可变长度 INT((N+7)/8) bytes：如 n=8 为1byte，n=9为2byte

	// 将表数据类型存入缓存
	tableMap[this.TableId] = *this
}
