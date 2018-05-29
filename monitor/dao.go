package monitor

import(
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
	"github.com/itgeniusshuai/mysql_slave/tools"
	"github.com/itgeniusshuai/mysql_slave/slave"
	"time"
)
var liteClientCache *sql.DB
const MONITOR_DB_NAME = "MonitorDb"

func GetSqliteClient(dbName string)(*sql.DB){
	if liteClientCache == nil{
		liteClient,err := sql.Open("sqlite3",dbName)
		if err != nil {
			tools.Println("open sqlite3 error:"+err.Error())
			return nil
		}
		liteClientCache = liteClient
	}
	return liteClientCache
}

func QueryMonitor(){

}

func InsertEvent(event slave.RowBinlogEvent){
	db := GetSqliteClient(MONITOR_DB_NAME)
	db.Exec("insert into RowEvent ()values (?,?,?,?)",event.ConnId,event.DbName,event.TableName,time.Now().Second())
}
