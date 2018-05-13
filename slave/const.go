package slave

const(
	CLIENT_LONG_PASSWORD	= 0x0001
	CLIENT_FOUND_ROWS	= 0x0002	
	CLIENT_LONG_FLAG	= 0x0004	
	CLIENT_CONNECT_WITH_DB	= 0x0008
	CLIENT_NO_SCHEMA	= 0x0010	
	CLIENT_COMPRESS	= 0x0020
	CLIENT_ODBC	= 0x0040
	CLIENT_LOCAL_FILES	= 0x0080
	CLIENT_IGNORE_SPACE	= 0x0100
	CLIENT_PROTOCOL_41	= 0x0200
	CLIENT_INTERACTIVE	= 0x0400
	CLIENT_SSL	= 0x0800
	CLIENT_IGNORE_SIGPIPE	= 0x1000
	CLIENT_TRANSACTIONS	= 0x2000
	CLIENT_RESERVED	= 0x4000
	CLIENT_SECURE_CONNECTION	= 0x8000
	CLIENT_MULTI_STATEMENTS	= 0x00010000
	CLIENT_MULTI_RESULTS	= 0x00020000
	CLIENT_PLUGIN_AUTH	= 0x80000


	COM_QUERY = 0x03
	COM_REGISTER_SLAVE = 0x15
	COM_BINLOG_DUMP = 0x12
	BINLOG_DUMP_NEVER_STOP = 0x00

	MAX_PACKET_SIZE = 1 << 24 - 1

	DEFAULT_POOL_SIZE = 3

	COM_PING = 0x0e


	// 事件类
	UNKNOWN_EVENT= 0
	START_EVENT_V3= 1
	QUERY_EVENT= 2
	STOP_EVENT= 3
	ROTATE_EVENT= 4
	INTVAR_EVENT= 5
	LOAD_EVENT= 6
	SLAVE_EVENT= 7
	CREATE_FILE_EVENT= 8
	APPEND_BLOCK_EVENT= 9
	EXEC_LOAD_EVENT= 10
	DELETE_FILE_EVENT= 11
	/**
	  NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
	  sql_ex  allowing multibyte TERMINATED BY etc; both types share the
	  same class (Load_event)
	*/
	NEW_LOAD_EVENT= 12
	RAND_EVENT= 13
	USER_VAR_EVENT= 14
	FORMAT_DESCRIPTION_EVENT= 15
	XID_EVENT= 16
	BEGIN_LOAD_QUERY_EVENT= 17
	EXECUTE_LOAD_QUERY_EVENT= 18

	TABLE_MAP_EVENT = 19

	/**
	  The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are
	  therefore obsolete.
	 */
	PRE_GA_WRITE_ROWS_EVENT = 20
	PRE_GA_UPDATE_ROWS_EVENT = 21
	PRE_GA_DELETE_ROWS_EVENT = 22

	/**
	  The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
	*/
	WRITE_ROWS_EVENT_V1 = 23
	UPDATE_ROWS_EVENT_V1 = 24
	DELETE_ROWS_EVENT_V1 = 25

	/**
	  Something out of the ordinary happened on the master
	 */
	INCIDENT_EVENT= 26

	/**
	  Heartbeat event to be send by master at its idle time
	  to ensure master's online status to slave
	*/
	HEARTBEAT_LOG_EVENT= 27

	/**
	  In some situations  it is necessary to send over ignorable
	  data to the slave: data that a slave can handle in case there
	  is code for handling it  but which can be ignored if it is not
	  recognized.
	*/
	IGNORABLE_LOG_EVENT= 28
	ROWS_QUERY_LOG_EVENT= 29

	/** Version 2 of the Row events */
	WRITE_ROWS_EVENT = 30
	UPDATE_ROWS_EVENT = 31
	DELETE_ROWS_EVENT = 32

	GTID_LOG_EVENT= 33
	ANONYMOUS_GTID_LOG_EVENT= 34

	PREVIOUS_GTIDS_LOG_EVENT= 35

	TRANSACTION_CONTEXT_EVENT= 36

	VIEW_CHANGE_EVENT= 37

	/* Prepared XA transaction terminal event similar to Xid */
	XA_PREPARE_LOG_EVENT= 38
		/**
	  Add new events here - right above this comment!
	  Existing events (except ENUM_END_EVENT) should never change their numbers
	*/
)
