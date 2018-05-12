package slave

import(
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
)
var liteClientCache *sql.DB
const MONITOR_DB_NAME = "MonitorDb"

func GetSqliteClient(dbName string)(*sql.DB){
	if liteClientCache == nil{
		liteClient,err := sql.Open("sqlite3",dbName)
		if err != nil {
			Println("open sqlite3 error:"+err.Error())
			return nil
		}
		liteClientCache = liteClient
	}
	return liteClientCache
}

func QueryMonitor(){

}
