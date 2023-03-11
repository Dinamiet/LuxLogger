package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
)

const (
	HOST = "mico.lan"
	PORT = "8000"
	TYPE = "tcp"
)

const (
	PREFIX             = 0x1AA1
	FUNCTION_HEARTBEAT = 0xC1
	FUNCTION_DATA      = 0xC2
	FUNCTION_READ      = 0xC3
	FUNCTION_WRITE     = 0xC4
)

const (
	DEVICE_READHOLD    = 0x03
	DEVICE_READINPUT   = 0x04
	DEVICE_WRITESINGLE = 0x06
	DEVICE_WRITEMULTI  = 0x10
)

type Header struct {
	Prefix          uint16   // 0..2
	ProtocolVersion uint16   // 2..4
	PacketLength    uint16   // 4..6
	Address         uint8    // 6
	Function        uint8    // 7
	SerialNumber    [10]byte // 8..18
	Reserved        uint16   // 18..20
}

func (head Header) String() string {
	return fmt.Sprintf("Header Prefix: %04X\nHeader Protocol: %04X\nHeader PacketLength: %d\nHeader Address: %02X\nHeader Function: %02X\nHeader Serial: %s\nHeader Reserved: %d\n",
		head.Prefix,
		head.ProtocolVersion,
		head.PacketLength,
		head.Address,
		head.Function,
		head.SerialNumber,
		head.Reserved)
}

type TranslatedData struct {
	Address        uint8
	DeviceFunction uint8
	SerialNumber   [10]byte
	Register       uint16
}

func (trans TranslatedData) String() string {
	return fmt.Sprintf("Translated Address: %02X\nTranslated DeviceFunction: %02X\nTranslated Serial Number %s\nTranslated Register: %04X\n",
		trans.Address,
		trans.DeviceFunction,
		trans.SerialNumber,
		trans.Register)
}

type LogData struct {
	Status          uint16
	PV1_Voltage     int16
	PV2_Voltage     int16
	PV3_Voltage     int16
	Battery_Voltage int16
	SOC             int8
	SOH             int8
}

func (log LogData) String() string {
	return fmt.Sprintf("Status: %04X\nPV1: %f\nPV2: %f\nPV3: %f\nBAT: %f\nSOC: %d\nSOH:%d\n",
		log.Status,
		float32(log.PV1_Voltage)/10.0,
		float32(log.PV2_Voltage)/10.0,
		float32(log.PV3_Voltage)/10.0,
		float32(log.Battery_Voltage)/10.0,
		log.SOC,
		log.SOH)
}

func test(frame []byte, length uint16) {
	header := Header{}
	reader := bytes.NewReader(frame)
	err := binary.Read(reader, binary.LittleEndian, &header)
	if err != nil {
		println("Error decoding:", err.Error())
	}

	fmt.Print(header)

	if PREFIX != header.Prefix {
		println("Invalid header prefix:", header.Prefix)
		return
	}

	if (length - 6) != header.PacketLength {
		println("Invalid length:", header.PacketLength)
		return
	}

	if header.Function == FUNCTION_HEARTBEAT {
		println("Function FUNCTION_HEARTBEAT")
	} else if header.Function == FUNCTION_DATA {
		println("Function FUNCTION_DATA")
		data := TranslatedData{}
		err := binary.Read(reader, binary.LittleEndian, &data)
		if err != nil {
			print("Error decoding:", err.Error())
			return
		}

		fmt.Print(data)

		// value_offset := 14

		if data.DeviceFunction == 0x01 {
			log := LogData{}
			err := binary.Read(reader, binary.LittleEndian, &log)
			if err != nil {
				print("Error decoding:", err.Error())
				return
			}

			fmt.Print(log)
		}

	} else if header.Function == FUNCTION_READ {
		println("Function FUNCTION_READ")
	} else if header.Function == FUNCTION_WRITE {
		println("Function FUNCTION_WRITE")
	} else {
		println("Function UNKNOWN: ", header.Function)
	}

	print("\n")
}

func main() {
	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)
	if err != nil {
		println("Resolve Failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP(TYPE, nil, tcpServer)
	if err != nil {
		println("Dail failed:", err.Error())
		os.Exit(2)
	}

	received := make([]byte, 1024)
	for {

		numRead, err := conn.Read(received)
		if err != nil {
			println("Read data failed:", err.Error())
			os.Exit(3)
		}
		data := hex.EncodeToString(received[:numRead])
		println("%d -> %s\n", numRead, data)
		test(received[:numRead], uint16(numRead))
	}
}
