package slave

import (
	"github.com/itgeniusshuai/go_common/common"
	"bytes"
	"encoding/binary"
	"errors"
	"time"
	"fmt"
	"strconv"
	"math"
	"strings"
	"reflect"
	"unsafe"
	"github.com/itgeniusshuai/mysql_slave/tools"
	"sync"
	"unicode"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"log"
	"runtime"
)

var tableMap = make(map[int]TableMapBinlogEvent,0)
var tableMapLock = sync.Mutex{}
var tableMeteMap = make(map[string]*TableMete,0)
var tableMeteMapLock = sync.Mutex{}
var connFormat = make(map[string]byte,0)
var formatDescLock = sync.Mutex{}
var db *sql.DB

type TableMete struct {
	ColumnNames []string
	ColumnTypes []string
	PkColumns []string
	PkColumnIndexes []int
}

type Position struct{
	Name string
	Pos uint32
}

func putTableMap(tableId int, event TableMapBinlogEvent){
	defer tableMapLock.Unlock()
	tableMapLock.Lock()
	tableMap[tableId] = event
}

func getTableMap(tableId int)TableMapBinlogEvent{
	defer tableMapLock.Unlock()
	tableMapLock.Lock()
	return tableMap[tableId]
}

func putTableMateMap(key string, tableMete *TableMete){
	defer tableMeteMapLock.Unlock()
	tableMeteMapLock.Lock()
	tableMeteMap[key] = tableMete
}

func getTableMateMap(key string)*TableMete{
	defer tableMeteMapLock.Unlock()
	tableMeteMapLock.Lock()
	return tableMeteMap[key]
}

func putConnFormatMap(connId string, checkNum byte){
	defer formatDescLock.Unlock()
	formatDescLock.Lock()
	connFormat[connId] = checkNum
}

func getConnFormatMap(connId string)byte{
	defer formatDescLock.Unlock()
	formatDescLock.Lock()
	return connFormat[connId]
}
func removeConnFormatMap(connId string){
	defer formatDescLock.Unlock()
	formatDescLock.Lock()
	delete(connFormat, connId)
}

// 事件体
type BinlogEvent interface {
	ParseEvent(bs []byte) bool
}

// 包含头和体
type BinlogEventStruct struct {
	BinlogHeader BinlogHeader
	BinlogEvent BinlogEvent
}

// 行事件
type RowBinlogEvent struct {
	TypeCode byte
	TableId int
	ColumnCount uint64
	FieldIsUsed []byte
	FieldUpdateUsed []byte // only update have
	DbName string
	TableName string
	TableMete *TableMete
	Pos uint
	RowDatas [][]interface{}

	ConnId string
	EventType EventType
	Conn *MysqlConnection
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

	Id int64
}

// 解析事件头每个事件都一样
func ParseBinlogHeader(bs []byte, conn *MysqlConnection) *BinlogHeader{
	size := 19 + 4 + 1
	if len(bs) < size{
		fmt.Println("size is %d",len(bs))
		return nil
	}
	pos := 4 + 1
	binlogHeader := BinlogHeader{}
	binlogHeader.TimeStamp = common.BytesToIntWithMin(bs[pos:pos + 4])
	pos += 4
	binlogHeader.TypeCode = bs[pos]
	binlogHeader.ServerId = bs[pos:pos+4]
	pos +=4
	binlogHeader.EventLength = common.BytesToIntWithMin(bs[pos:pos+4])
	pos +=4
	binlogHeader.NextPosition = common.BytesToIntWithMin(bs[pos:pos+4])
	pos +=4
	binlogHeader.Flags = bs[pos:pos+2]
	if binlogHeader.EventLength < size{
		return nil
	}
	// 封装id，不同连接的一个消息要相同，并且有递增顺序 bs[3]就是递增序号
	var curSeq = int(bs[3])
	currentTimeStamp := binlogHeader.TimeStamp
	if currentTimeStamp != conn.LastTimeStamp {
		conn.MulFactor = 0
	}else if conn.LastSeq == 255 {
		conn.MulFactor ++
	}
	// 每个连接不共享的三个变量，用于生成每个事件的ID
	conn.LastSeq = curSeq
	conn.LastTimeStamp = currentTimeStamp
	binlogHeader.Id = (int64(currentTimeStamp) << 32) | (int64(curSeq)+256*conn.MulFactor)
	//fmt.Println(fmt.Sprintf("timepstamp[%d]seq[%d]finalSeq[%d]",binlogHeader.TimeStamp,int(curSeq),(int64(curSeq)+255*conn.MulFactor)))
	//fmt.Println(fmt.Sprintf("id[%d]",binlogHeader.Id))
	return &binlogHeader
}

// 解析事件（包含头和体）
func ParseEvent(bs []byte,conn *MysqlConnection) *BinlogEventStruct{
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if r, ok := r.(runtime.Error); ok {
				err = errors.New(r.Error())
			}
			if s, ok := r.(string); ok {
				err =  errors.New(s)
			}
		}
	}()
	//tools.Println("received event %s",common.BytesToStr(bs))
	binlogEventStruct := BinlogEventStruct{}
	header := ParseBinlogHeader(bs,conn)
	if header == nil{
		return nil
	}
	binlogEventStruct.BinlogHeader = *header
	var binlogEvent BinlogEvent
	// 只解析增删改查的行事件
	switch header.TypeCode {
	case WRITE_ROWS_EVENT,WRITE_ROWS_EVENT_V1:
		binlogEvent = &RowBinlogEvent{TypeCode:header.TypeCode,EventType:BINLOG_WRITE,Conn:conn}
	case UPDATE_ROWS_EVENT,UPDATE_ROWS_EVENT_V1:
		binlogEvent = &RowBinlogEvent{TypeCode:header.TypeCode,EventType:BINLOG_UPDATE,Conn:conn}
	case DELETE_ROWS_EVENT,DELETE_ROWS_EVENT_V1:
		binlogEvent = &RowBinlogEvent{TypeCode:header.TypeCode,EventType:BINLOG_DELETE,Conn:conn}
	case TABLE_MAP_EVENT:
		binlogEvent = &TableMapBinlogEvent{}
	case FORMAT_DESCRIPTION_EVENT:
		// 第一个binlog事件
		binlogEvent = &FormatDescEvent{}
	default:
		return nil
	}
	flag := binlogEvent.ParseEvent(bs)
	if(!flag){
		return nil
	}
	binlogEventStruct.BinlogEvent = binlogEvent
	return &binlogEventStruct
}

// 具体事件的解析实现
func (this *RowBinlogEvent) ParseEvent(bs []byte) bool{
	pos := 19 + 4 + 1
	this.TableId = common.BytesToIntWithMin(bs[pos:pos+6])
	pos += 6 + 2 // 2 bytes Reserved for future use.
	this.FieldIsUsed = bs[pos:pos+2]
	pos += 2
	var n int
	this.ColumnCount, _, n = LengthEncodedInt(bs[pos:])
	pos += n
	num := getBitSetLength(this.ColumnCount)
	this.FieldIsUsed = bs[pos:pos+int(num)]
	pos += int(num)
	// 更新类的事件多一个update used
	if this.TypeCode == UPDATE_ROWS_EVENT || this.TypeCode == UPDATE_ROWS_EVENT_V1{
		this.FieldUpdateUsed = bs[pos:pos+int(num)]
		pos += int(num)
	}
	// 每个recored 包含一个是否为空的bitset及
	// 获取列信息
	tableMapEvent := getTableMap(this.TableId)
	this.DbName = tableMapEvent.DbName
	this.TableName = tableMapEvent.TableName
	if getConnFormatMap(this.ConnId) == BINLOG_CHECKSUM_ALG_CRC32{
		bs = bs[:len(bs)-4]
	}
	for pos < len(bs){
		n,_= this.decodeRows(bs[pos:], &tableMapEvent, this.FieldIsUsed)
		pos += n

		if  this.TypeCode == UPDATE_ROWS_EVENT || this.TypeCode == UPDATE_ROWS_EVENT_V1{
			n,_= this.decodeRows(bs[pos:], &tableMapEvent, this.FieldUpdateUsed)
			pos += n
		}
	}

	// 获取表字段
	this.TableMete = this.Conn.GetColumns(this.DbName,this.TableName)
	return true;

}

func (this *MysqlConnection) GetMasterStatus()*Position{
	db := this.GetConn()
	r,err := db.Query("show master status")
	if(err != nil){
		fmt.Println(err)
	}
	var file string
	var position uint32
	var db1 string
	var ignoreDb string
	var gidset string
	if r.Next(){
		r.Scan(&file,&position,&db1,&ignoreDb,&gidset)
	}
	pos := Position{Name:file,Pos:position}
	return &pos
}

func (this *MysqlConnection) GetConn()*sql.DB{

	if (db == nil) || db.Ping() != nil{
		dateSourceName := this.User+":"+this.Pwd+"@tcp("+this.Host+":"+common.IntToStr(this.Port)+")/"
		db,_ =sql.Open("mysql", dateSourceName)
		//this.Db = db
		return db;
	}

	return db
}

func (this *MysqlConnection)GetColumns(dbName string,tableName string)*TableMete{
	key := dbName+"_"+tableName
	tableMete := getTableMateMap(key)
	//if tableMete != nil{
	//	return tableMete
	//}
	db := this.GetConn()
	defer db.Close()
	r, err := db.Query(fmt.Sprintf("show full columns from `%s`.`%s`", dbName,tableName))
	if(err != nil){
		fmt.Print(err)
		return nil
	}
	//columnNames := make([]string,0)
	columns,_ := r.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	columnNames := make([]string,0)
	columnTypes := make([]string,0)
	pkColumns := make([]string,0)
	pkColumnIndexes := make([]int,0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for r.Next() {
		err = r.Scan(scanArgs...)
		if err != nil {
			log.Fatal(err)
		}
		var value string
		for k, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			if k == 0{
				columnNames = append(columnNames, value)
			}
			if k == 1{
				columnTypes = append(columnTypes, value)
			}
			if k == 4{
				if value == "PRI" {
					pkColumns = append(pkColumns, columnNames[len(columnNames)-1])
					pkColumnIndexes = append(pkColumnIndexes, len(columnNames)-1)
				}
			}
		}

	}
	tableMete = new(TableMete)
	tableMete.ColumnNames = columnNames
	tableMete.ColumnTypes = columnTypes
	tableMete.PkColumns = pkColumns
	tableMete.PkColumnIndexes = pkColumnIndexes

	putTableMateMap(key,tableMete)
	return tableMete
}

func (e *RowBinlogEvent) decodeRows(data []byte, table *TableMapBinlogEvent, bitmap []byte) (int, error) {
	row := make([]interface{}, e.ColumnCount)

	pos := 0

	// refer: https://github.com/alibaba/canal/blob/c3e38e50e269adafdd38a48c63a1740cde304c67/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L63
	count := 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if isBitSet(bitmap, i) {
			count++
		}
	}
	count = (count + 7) / 8

	nullBitmap := data[pos : pos+count]
	pos += count

	nullbitIndex := 0

	var n int
	var err error
	for i := 0; i < int(e.ColumnCount); i++ {
		if !isBitSet(bitmap, i) {
			continue
		}

		isNull := (uint32(nullBitmap[nullbitIndex/8]) >> uint32(nullbitIndex%8)) & 0x01
		nullbitIndex++

		if isNull > 0 {
			row[i] = nil
			continue
		}

		row[i], n, err = e.decodeValue(data[pos:], table.ColumnTypes[i], table.MetaBlock[i])

		if err != nil {
			return 0, err
		}
		pos += n
	}

	e.RowDatas = append(e.RowDatas, row)
	return pos, nil
}

func (e *RowBinlogEvent) parseFracTime(t interface{}) interface{} {
	v, ok := t.(fracTime)
	if !ok {
		return t
	}

		// Don't parse time, return string directly
	return v.String()
}

// see mysql sql/log_event.cc log_event_print_value
func (e *RowBinlogEvent) decodeValue(data []byte, tp byte, meta uint16) (v interface{}, n int, err error) {
	var length int = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = byte(b0 | 0x30)
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
		v = ParseBinaryInt32(data)
	case MYSQL_TYPE_TINY:
		n = 1
		v = ParseBinaryInt8(data)
	case MYSQL_TYPE_SHORT:
		n = 2
		v = ParseBinaryInt16(data)
	case MYSQL_TYPE_INT24:
		n = 3
		v = ParseBinaryInt24(data)
	case MYSQL_TYPE_LONGLONG:
		n = 8
		v = ParseBinaryInt64(data)
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n, err = decodeDecimal(data, int(prec), int(scale))
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = ParseBinaryFloat32(data)
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = ParseBinaryFloat64(data)
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8

		//use int64 for bit
		v, err = decodeBit(data, int(nbits), int(n))
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		t := binary.LittleEndian.Uint32(data)
		v = e.parseFracTime(fracTime{time.Unix(int64(t), 0), 0})
	case MYSQL_TYPE_TIMESTAMP2:
		v, n, err = decodeTimestamp2(data, meta)
		v = e.parseFracTime(v)
	case MYSQL_TYPE_DATETIME:
		n = 8
		i64 := binary.LittleEndian.Uint64(data)
		d := i64 / 1000000
		t := i64 % 1000000
		v = e.parseFracTime(fracTime{time.Date(int(d/10000),
			time.Month((d%10000)/100),
			int(d%100),
			int(t/10000),
			int((t%10000)/100),
			int(t%100),
			0,
			time.UTC), 0})
	case MYSQL_TYPE_DATETIME2:
		v, n, err = decodeDatetime2(data, meta)
		v = e.parseFracTime(v)
	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			sign := ""
			if i32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
		}
	case MYSQL_TYPE_TIME2:
		v, n, err = decodeTime2(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "0000-00-00"
		} else {
			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		}

	case MYSQL_TYPE_YEAR:
		n = 1
		v = int(data[0]) + 1900
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = int64(data[0])
			n = 1
		case 2:
			v = int64(binary.BigEndian.Uint16(data))
			n = 2
		default:
			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
	case MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		nbits := n * 8

		v, err = decodeBit(data, nbits, n)
	case MYSQL_TYPE_BLOB:
		v, n, err = decodeBlob(data, meta)
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		v, n = decodeString(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeString(data, length)
	case MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		length = int(FixedLengthInt(data[0:meta]))
		n = length + int(meta)
		//v, err = decodeJsonBinary(data[meta:n])
	case MYSQL_TYPE_GEOMETRY:
		// MySQL saves Geometry as Blob in binlog
		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
		// MySQL GeoFromWKB or others to create the geometry data.
		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
		// I also find some go libs to handle WKB if possible
		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
		v, n, err = decodeBlob(data, meta)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}
	return
}


type TableMapBinlogEvent struct {
	TableId int
	DbNameLength byte
	DbName string
	TableNameLength byte
	TableName string
	ColumnCount uint64
	ColumnTypes []byte
	MetaBlockLength uint64
	MetaBlock []uint16
	FieldIsNull []byte

}

func (this *TableMapBinlogEvent)ParseEvent(bs []byte)bool{
	pos := 19 + 4 + 1
	this.TableId = common.BytesToIntWithMin(bs[pos:pos+6])
	pos += 6 + 2 // 2 bytes Reserved for future use.
	// 存入表映射
	this.DbNameLength = bs[pos]
	pos ++
	// 数据库名称可变长度
	this.DbName = common.BytesToStr(bs[pos:int(this.DbNameLength)+pos])
	pos = int(this.DbNameLength)+pos+1
	// 表名长度 1byte
	this.TableNameLength = bs[pos]
	pos ++
	// 表名 可变长度
	this.TableName = common.BytesToStr(bs[pos:int(this.TableNameLength)+pos])
	pos = int(this.TableNameLength)+pos+1
	// 表中列数 int
	count,_,n := LengthEncodedInt(bs[pos:])
	this.ColumnCount = count
	pos += n
	// 列类型 可变长度，每列一个字节 todo
	this.ColumnTypes = bs[pos: pos+int(this.ColumnCount)]
	pos = pos+int(this.ColumnCount)
	// 元数据块长度 int
	var metaData []byte
	metaData,_,n,_ = tools.LengthEnodedString(bs[pos:])
	//count,_,n = LengthEncodedInt(bs[pos:])
	//this.MetaBlockLength = count
	//pos += int(count)
	// 元数据块 可变长度
	this.decodeMeta(metaData)
	// 是否每列为空 可变长度 INT((N+7)/8) bytes：如 n=8 为1byte，n=9为2byte
	// 将表数据类型存入缓存
	putTableMap(this.TableId,*this)
	return false;
}

func (e *TableMapBinlogEvent) decodeMeta(data []byte) error {
	pos := 0
	e.MetaBlock = make([]uint16, e.ColumnCount)
	for i, t := range e.ColumnTypes {
		switch t {
		case MYSQL_TYPE_STRING:
			var x uint16 = uint16(data[pos]) << 8 //real type
			x += uint16(data[pos+1])              //pack or field length
			e.MetaBlock[i] = x
			pos += 2
		case MYSQL_TYPE_NEWDECIMAL:
			var x uint16 = uint16(data[pos]) << 8 //precision
			x += uint16(data[pos+1])              //decimals
			e.MetaBlock[i] = x
			pos += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.MetaBlock[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY,
			MYSQL_TYPE_JSON:
			e.MetaBlock[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.MetaBlock[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return errors.New("unsupport type in binlog "+common.ByteToStr(t))
		default:
			e.MetaBlock[i] = 0
		}
	}

	return nil
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {

	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

		// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

		// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

		// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func getBitSetLength(columnCount uint64)int{
	return (int(columnCount) + 7)/8
}

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

func decodeString(data []byte, length int) (v string, n int) {
	if length < 256 {
		length = int(data[0])

		n = int(length) + 1
		v = String(data[1:n])
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = String(data[2:n])
	}

	return
}

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decodeDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	databuff := make([]byte, size)
	for i := 0; i < size; i++ {
		databuff[i] = data[i] ^ mask
	}
	value = uint32(BFixedLengthInt(databuff))
	return
}

func decodeDecimal(data []byte, precision int, decimals int) (float64, int, error) {
	//see python mysql replication and https://github.com/jeremycole/mysql_binlog
	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	//must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := uint32(data[0])
	var res bytes.Buffer
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	//clear sign
	data[0] ^= 0x80

	pos, value := decodeDecimalDecompressValue(compIntegral, data, uint8(mask))
	res.WriteString(fmt.Sprintf("%d", value))

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := decodeDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		pos += size
	}

	f, err := strconv.ParseFloat(String(res.Bytes()), 64)
	return f, pos, err
}

func decodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(data))
		case 3:
			value = int64(BFixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.BigEndian.Uint32(data))
		case 5:
			value = int64(BFixedLengthInt(data[0:5]))
		case 6:
			value = int64(BFixedLengthInt(data[0:6]))
		case 7:
			value = int64(BFixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.BigEndian.Uint64(data))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

func decodeTimestamp2(data []byte, dec uint16) (interface{}, int, error) {
	//get timestamp binary length
	n := int(4 + (dec+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)
	switch dec {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(BFixedLengthInt(data[4:7]))
	}

	if sec == 0 {
		return formatZeroTime(int(usec), int(dec)), n, nil
	}

	return fracTime{time.Unix(sec, usec*1000), int(dec)}, n, nil
}

const DATETIMEF_INT_OFS int64 = 0x8000000000

func decodeDatetime2(data []byte, dec uint16) (interface{}, int, error) {
	//get datetime binary length
	n := int(5 + (dec+1)/2)

	intPart := int64(BFixedLengthInt(data[0:5])) - DATETIMEF_INT_OFS
	var frac int64 = 0

	switch dec {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(BFixedLengthInt(data[5:8]))
	}

	if intPart == 0 {
		return formatZeroTime(int(frac), int(dec)), n, nil
	}

	tmp := intPart<<24 + frac
	//handle sign???
	if tmp < 0 {
		tmp = -tmp
	}

	// var secPart int64 = tmp % (1 << 24)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int((hms >> 12))

	return fracTime{time.Date(year, time.Month(month), day, hour, minute, second, int(frac*1000), time.UTC), int(dec)}, n, nil
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000

func decodeTime2(data []byte, dec uint16) (string, int, error) {
	//time  binary length
	n := int(3 + (dec+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch dec {
	case 1:
	case 2:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		if intPart < 0 && frac > 0 {
			/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

			     Disk value  intpart frac   Time value   Memory value
			     800000.00    0      0      00:00:00.00  0000000000.000000
			     7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
			     7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
			     7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
			     7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
			     7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

			     Formula to convert fractional part from disk format
			     (now stored in "frac" variable) to absolute value: "0x100 - frac".
			     To reconstruct in-memory value, we shift
			     to the next integer value and then substruct fractional part.
			*/
			intPart++     /* Shift to the next integer value */
			frac -= 0x100 /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3:
	case 4:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac > 0 {
			/*
			   Fix reverse fractional part order: "0x10000 - frac".
			   See comments for FSP=1 and FSP=2 above.
			*/
			intPart++       /* Shift to the next integer value */
			frac -= 0x10000 /* -(0x10000-frac) */
		}
		tmp = intPart<<24 + frac*100

	case 5:
	case 6:
		tmp = int64(BFixedLengthInt(data[0:6])) - TIMEF_OFS
	default:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", n, nil
	}

	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) /* 10 bits starting at 12th */
	minute := (hms >> 6) % (1 << 6) /* 6 bits starting at 6th   */
	second := hms % (1 << 6)        /* 6 bits starting at 0th   */
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart), n, nil
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), n, nil
}

func decodeBlob(data []byte, meta uint16) (v []byte, n int, err error) {
	var length int
	switch meta {
	case 1:
		length = int(data[0])
		v = data[1 : 1+length]
		n = length + 1
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
		v = data[2 : 2+length]
		n = length + 2
	case 3:
		length = int(FixedLengthInt(data[0:3]))
		v = data[3 : 3+length]
		n = length + 3
	case 4:
		length = int(binary.LittleEndian.Uint32(data))
		v = data[4 : 4+length]
		n = length + 4
	default:
		err = fmt.Errorf("invalid blob packlen = %d", meta)
	}

	return
}

// little endian
func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

// big endian
func BFixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

func ParseBinaryInt8(data []byte) int8 {
	return int8(data[0])
}
func ParseBinaryUint8(data []byte) uint8 {
	return data[0]
}

func ParseBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}
func ParseBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func ParseBinaryInt24(data []byte) int32 {
	u32 := uint32(ParseBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}
func ParseBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func ParseBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}
func ParseBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}
func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ParseBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ParseBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

var (
	fracTimeFormat []string
)

// fracTime is a help structure wrapping Golang Time.
type fracTime struct {
	time.Time

	// Dec must in [0, 6]
	Dec int
}

func (t fracTime) String() string {
	return t.Format(fracTimeFormat[t.Dec])
}

func formatZeroTime(frac int, dec int) string {
	if dec == 0 {
		return "0000-00-00 00:00:00"
	}

	s := fmt.Sprintf("0000-00-00 00:00:00.%06d", frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func init() {
	fracTimeFormat = make([]string, 7)
	fracTimeFormat[0] = "2006-01-02 15:04:05"

	for i := 1; i <= 6; i++ {
		fracTimeFormat[i] = fmt.Sprintf("2006-01-02 15:04:05.%s", strings.Repeat("0", i))
	}
}

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

type FormatDescEvent struct {
	Version uint16
	//len = 50
	ServerVersion          []byte
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte

	// 0 is off, 1 is for CRC32, 255 is undefined
	ChecksumAlgorithm byte
	ConnId string
}
var(
	checksumVersionSplitMysql   []int = []int{5, 6, 1}
	checksumVersionProductMysql int   = (checksumVersionSplitMysql[0]*256+checksumVersionSplitMysql[1])*256 + checksumVersionSplitMysql[2]
)

func (e *FormatDescEvent)ParseEvent(bs []byte) bool{
	pos := 19 + 4 + 1
	e.Version = binary.LittleEndian.Uint16(bs[pos:])
	pos += 2

	e.ServerVersion = make([]byte, 50)
	copy(e.ServerVersion, bs[pos:])
	pos += 50

	e.CreateTimestamp = binary.LittleEndian.Uint32(bs[pos:])
	pos += 4

	e.EventHeaderLength = bs[pos]
	pos++

	//if e.EventHeaderLength != byte(EventHeaderSize) {
	//	return errors.Errorf("invalid event header length %d, must 19", e.EventHeaderLength)
	//}

	server := string(e.ServerVersion)
	checksumProduct := checksumVersionProductMysql

	if calcVersionProduct(server) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		e.ChecksumAlgorithm = bs[len(bs)-5]
		e.EventTypeHeaderLengths = bs[pos : len(bs)-5]
	} else {
		e.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		e.EventTypeHeaderLengths = bs[pos:]
	}
	tools.Println("algorithm %d",e.ChecksumAlgorithm)
	putConnFormatMap(e.ConnId,e.ChecksumAlgorithm)
	return false;

}

func calcVersionProduct(server string) int {
	versionSplit := splitServerVersion(server)

	return ((versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2])
}

func splitServerVersion(server string) []int {
	seps := strings.Split(server, ".")
	if len(seps) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(seps[0])
	y, _ := strconv.Atoi(seps[1])

	index := 0
	for i, c := range seps[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(seps[2][0:index])

	return []int{x, y, z}
}


