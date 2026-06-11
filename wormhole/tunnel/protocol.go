package tunnel

import (
	"encoding/binary"
	"fmt"
)

const (
	MsgOpen  byte = 0x01
	MsgData  byte = 0x02
	MsgClose byte = 0x03

	headerSize = 1 + 8 + 4
)

type Message struct {
	Type    byte
	ConnID  uint64
	Payload []byte
}

func EncodeOpen(connID uint64, addr string) []byte {
	return encodeMsg(MsgOpen, connID, []byte(addr))
}

func EncodeData(connID uint64, data []byte) []byte {
	return encodeMsg(MsgData, connID, data)
}

func EncodeClose(connID uint64) []byte {
	return encodeMsg(MsgClose, connID, nil)
}

func Decode(raw []byte) (Message, error) {
	if len(raw) < headerSize {
		return Message{}, fmt.Errorf("tunnel: message too short: %d", len(raw))
	}

	msgType := raw[0]
	connID := binary.BigEndian.Uint64(raw[1:9])
	payloadLen := binary.BigEndian.Uint32(raw[9:13])

	if len(raw) < headerSize+int(payloadLen) {
		return Message{}, fmt.Errorf("tunnel: incomplete message: have %d, need %d", len(raw), headerSize+int(payloadLen))
	}

	payload := make([]byte, payloadLen)
	copy(payload, raw[headerSize:headerSize+int(payloadLen)])

	switch msgType {
	case MsgOpen, MsgData, MsgClose:
	default:
		return Message{}, fmt.Errorf("tunnel: unknown message type: 0x%02x", msgType)
	}

	return Message{Type: msgType, ConnID: connID, Payload: payload}, nil
}

func encodeMsg(msgType byte, connID uint64, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	buf[0] = msgType
	binary.BigEndian.PutUint64(buf[1:9], connID)
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(payload)))
	copy(buf[headerSize:], payload)
	return buf
}
