package tunnel

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeOpen(t *testing.T) {
	encoded := EncodeOpen(42, "127.0.0.1:8080")
	msg, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != MsgOpen {
		t.Fatalf("expected MsgOpen, got 0x%02x", msg.Type)
	}
	if msg.ConnID != 42 {
		t.Fatalf("expected connID=42, got %d", msg.ConnID)
	}
	if string(msg.Payload) != "127.0.0.1:8080" {
		t.Fatalf("expected addr '127.0.0.1:8080', got %q", string(msg.Payload))
	}
}

func TestEncodeDecodeData(t *testing.T) {
	payload := []byte("hello tunnel")
	encoded := EncodeData(7, payload)
	msg, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != MsgData {
		t.Fatalf("expected MsgData, got 0x%02x", msg.Type)
	}
	if msg.ConnID != 7 {
		t.Fatalf("expected connID=7, got %d", msg.ConnID)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Fatalf("expected %q, got %q", payload, msg.Payload)
	}
}

func TestEncodeDecodeClose(t *testing.T) {
	encoded := EncodeClose(99)
	msg, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != MsgClose {
		t.Fatalf("expected MsgClose, got 0x%02x", msg.Type)
	}
	if msg.ConnID != 99 {
		t.Fatalf("expected connID=99, got %d", msg.ConnID)
	}
	if len(msg.Payload) != 0 {
		t.Fatalf("expected empty payload, got %q", msg.Payload)
	}
}

func TestDecodeTooShort(t *testing.T) {
	_, err := Decode([]byte{0x01, 0x00})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestDecodeUnknownType(t *testing.T) {
	buf := make([]byte, headerSize)
	buf[0] = 0xFF
	_, err := Decode(buf)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestDecodeIncompletePayload(t *testing.T) {
	buf := make([]byte, headerSize+2)
	buf[0] = MsgData
	buf[9] = 0
	buf[10] = 0
	buf[11] = 0
	buf[12] = 10
	buf[13] = 'a'
	buf[14] = 'b'
	_, err := Decode(buf)
	if err == nil {
		t.Fatal("expected error for incomplete payload")
	}
}
