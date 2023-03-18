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
	_              uint8
}

func (trans TranslatedData) String() string {
	return fmt.Sprintf("Translated Address: %02X\nTranslated DeviceFunction: %02X\nTranslated Serial Number %s\nTranslated Register: %04X\n",
		trans.Address,
		trans.DeviceFunction,
		trans.SerialNumber,
		trans.Register)
}

type LogDataSection1 struct {
	Status                      uint16
	PV1_Voltage                 int16
	PV2_Voltage                 int16
	PV3_Voltage                 int16
	Battery_Voltage             int16
	SOC                         int8
	SOH                         int8
	_                           int16
	PV1_Power                   int16
	PV2_Power                   int16
	PV3_Power                   int16
	Charge_Power                int16
	Discharge_Power             int16
	Voltage_AC_R                int16
	Voltage_AC_S                int16
	Voltage_AC_T                int16
	Frequency_Grid              int16
	ActiveCharge_Power          int16
	ActiveInverter_Power        int16
	Inductor_Current            int16
	Grid_Power_Factor           int16
	Voltage_EPS_R               int16
	Voltage_EPS_S               int16
	Voltage_EPS_T               int16
	Frequency_EPS               int16
	Active_EPS_Power            int16
	Apparent_EPS_Power          int16
	Power_To_Grid               int16
	Power_From_Grid             int16
	PV1_Energy_Today            int16
	PV2_Energy_Today            int16
	PV3_Energy_Today            int16
	ActiveInverter_Energy_Today int16
	AC_Charging_Today           int16
	Charging_Today              int16
	Discharging_Today           int16
	EPS_Today                   int16
	Exported_Today              int16
	Grid_Today                  int16
	Bus1_Voltage                int16
	Bus2_Voltage                int16
}

type LogDataSection2 struct {
	PV1_Energy_Total            int32
	PV2_Energy_Total            int32
	PV3_Energy_Total            int32
	ActiveInverter_Energy_Total int32
	AC_Charging_Total           int32
	Charging_Total              int32
	Discharging_Total           int32
	EPS_Total                   int32
	Exported_Total              int32
	Grid_Total                  int32
	FaultCode                   uint32
	WarningCode                 uint32
	Inner_Temperature           int16
	Radiator1_Temperature       int16
	Radiator2_Temperature       int16
	Battery_Temperature         int16
	_                           int16
	Runtime                     int32
	_                           [18]byte
}

type LogDataSection3 struct {
	BatteryComType               int16
	BMS_Max_Charge_Current       int16
	BMS_Max_Discharge_Current    int16
	BMS_Charge_Voltage_Reference int16
	BMS_Discharge_Cutoff         int16
	BMS_Status                   [10]uint16
	BMS_Inverter_Status          int16
	Battery_Parallel_Count       int16
	Battery_Capacity             int16
	Battery_Current              int16
	BMS_Event1                   int16
	BMS_Event2                   int16
	MaxCell_Voltage              int16
	MinCell_Voltage              int16
	MaxCell_Temp                 int16
	MinCell_Temp                 int16
	BMS_FW_Update_State          int16
	Cycle_Count                  int16
	BatteryInverter_Voltage      int16
}

type LogData struct {
	Section1 LogDataSection1
	Section2 LogDataSection2
	Section3 LogDataSection3
}

func (log LogData) String() string {
	return fmt.Sprintf("Status: %04X\nPV1: %f\nPV2: %f\nPV3: %f\nBAT: %f\nSOC: %d\nSOH: %d\n",
		log.Section1.Status,
		float32(log.Section1.PV1_Voltage)/10.0,
		float32(log.Section1.PV2_Voltage)/10.0,
		float32(log.Section1.PV3_Voltage)/10.0,
		float32(log.Section1.Battery_Voltage)/10.0,
		log.Section1.SOC,
		log.Section1.SOH)
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

		if data.DeviceFunction == DEVICE_READINPUT {
			log := LogData{}
			if data.Register == 0 {
				if header.PacketLength == 285 {
					fmt.Printf("ReadAll: %d\n", header.PacketLength)
					err := binary.Read(reader, binary.LittleEndian, &log)
					if err != nil {
						print("Error decoding:", err.Error())
						return
					}
					fmt.Print(log)
				} else if header.PacketLength == 111 {
					fmt.Printf("Read1: %d\n", header.PacketLength)
					err := binary.Read(reader, binary.LittleEndian, &log.Section1)
					if err != nil {
						print("Error decoding:", err.Error())
						return
					}
					fmt.Print(log)
				}
			} else if data.Register == 40 && header.PacketLength == 111 {
				fmt.Printf("Read2: %d\n", length)
				err := binary.Read(reader, binary.LittleEndian, &log.Section2)
				if err != nil {
					print("Error decoding:", err.Error())
					return
				}
				fmt.Print(log)
			} else if data.Register == 80 && header.PacketLength == 111 {
				fmt.Printf("Read3: %d\n", length)
				err := binary.Read(reader, binary.LittleEndian, &log.Section3)
				if err != nil {
					print("Error decoding:", err.Error())
					return
				}
				fmt.Print(log)
			}
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
		go test(received[:numRead], uint16(numRead))
	}
}
