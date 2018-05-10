package mysql

type ServerShackPacket struct {
	ProtocolVersionNum byte
	ProtocolVersion string
	ServerThreadId int
	ChallengeRandNum []byte
	FullValue byte
	PowerFlagLow []byte
	Charset byte
	ServerStatus []byte
	PowerFlagHigh []byte
	ChallengeLength byte
	FullValue1 []byte
	ChallengeRandNum2 []byte
}

type ClientShackPacket struct{
	PowerFlagLow []byte
	PowerFlayExt []byte
	MaxMsgLength []byte
	Charset byte
	FullValue []byte
	UserName string
	ChallengeAuthData []byte
	DbName string
}

type ProtocolPacket struct{
	PacketLen int
	PacketNum byte
	Packet interface{}
}
