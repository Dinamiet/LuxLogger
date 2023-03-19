package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
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

type LogDataRawSection1 struct {
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

type LogDataSection1 struct {
	Loaded                      bool
	Status                      uint16
	PV1_Voltage                 float32
	PV2_Voltage                 float32
	PV3_Voltage                 float32
	Battery_Voltage             float32
	SOC                         float32
	SOH                         float32
	PV1_Power                   float32
	PV2_Power                   float32
	PV3_Power                   float32
	Charge_Power                float32
	Discharge_Power             float32
	Voltage_AC_R                float32
	Voltage_AC_S                float32
	Voltage_AC_T                float32
	Frequency_Grid              float32
	ActiveCharge_Power          float32
	ActiveInverter_Power        float32
	Inductor_Current            float32
	Grid_Power_Factor           float32
	Voltage_EPS_R               float32
	Voltage_EPS_S               float32
	Voltage_EPS_T               float32
	Frequency_EPS               float32
	Active_EPS_Power            float32
	Apparent_EPS_Power          float32
	Power_To_Grid               float32
	Power_From_Grid             float32
	PV1_Energy_Today            float32
	PV2_Energy_Today            float32
	PV3_Energy_Today            float32
	ActiveInverter_Energy_Today float32
	AC_Charging_Today           float32
	Charging_Today              float32
	Discharging_Today           float32
	EPS_Today                   float32
	Exported_Today              float32
	Grid_Today                  float32
	Bus1_Voltage                float32
	Bus2_Voltage                float32
}

type LogDataRawSection2 struct {
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
	Runtime                     uint32
	_                           [18]byte
}

type LogDataSection2 struct {
	Loaded                      bool
	PV1_Energy_Total            float32
	PV2_Energy_Total            float32
	PV3_Energy_Total            float32
	ActiveInverter_Energy_Total float32
	AC_Charging_Total           float32
	Charging_Total              float32
	Discharging_Total           float32
	EPS_Total                   float32
	Exported_Total              float32
	Grid_Total                  float32
	FaultCode                   uint32
	WarningCode                 uint32
	Inner_Temperature           float32
	Radiator1_Temperature       float32
	Radiator2_Temperature       float32
	Battery_Temperature         float32
	Runtime                     uint32
}

type LogDataRawSection3 struct {
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

type LogDataSection3 struct {
	Loaded                       bool
	BatteryComType               int16
	BMS_Max_Charge_Current       float32
	BMS_Max_Discharge_Current    float32
	BMS_Charge_Voltage_Reference float32
	BMS_Discharge_Cutoff         float32
	BMS_Status                   [10]uint16
	BMS_Inverter_Status          int16
	Battery_Parallel_Count       int16
	Battery_Capacity             float32
	Battery_Current              float32
	BMS_Event1                   int16
	BMS_Event2                   int16
	MaxCell_Voltage              float32
	MinCell_Voltage              float32
	MaxCell_Temp                 float32
	MinCell_Temp                 float32
	BMS_FW_Update_State          int16
	Cycle_Count                  int16
	BatteryInverter_Voltage      float32
}

type LogDataRaw struct {
	Section1 LogDataRawSection1
	Section2 LogDataRawSection2
	Section3 LogDataRawSection3
}

type LogData struct {
	Raw      LogDataRaw
	Section1 LogDataSection1
	Section2 LogDataSection2
	Section3 LogDataSection3
}

func (log LogData) String() string {
	json, err := json.MarshalIndent(log, "", "\t")
	if err != nil {
		fmt.Println(err)
		return ""
	}

	return string(json)
}

func (log *LogData) Decode(frame []byte, length uint16) bool {
	header := Header{}
	reader := bytes.NewReader(frame)
	err := binary.Read(reader, binary.LittleEndian, &header)
	if err != nil {
		println("Error reading header:", err.Error())
		return false
	}

	if PREFIX != header.Prefix {
		println("Invalid header prefix:", header.Prefix)
		return false
	}

	if (length - 6) != header.PacketLength {
		println("Invalid length:", header.PacketLength)
		return false
	}

	if header.Function != FUNCTION_DATA {
		println("Unhandled header function:", header.Function)
		return false
	}

	data := TranslatedData{}
	err = binary.Read(reader, binary.LittleEndian, &data)
	if err != nil {
		println("Error reading Translated data:", err.Error())
		return false
	}

	if data.DeviceFunction != DEVICE_READINPUT {
		println("Unhandled device function:", data.DeviceFunction)
		return false
	}

	switch {
	case data.Register == 0 && header.PacketLength == 285:
		err = binary.Read(reader, binary.LittleEndian, &log.Raw)
		if err != nil {
			print("Error reading LogData:", err.Error())
			return false
		}
		log.Section1.Loaded = true
		log.Section2.Loaded = true
		log.Section3.Loaded = true
	case data.Register == 0 && header.PacketLength == 111:
		err = binary.Read(reader, binary.LittleEndian, &log.Raw.Section1)
		if err != nil {
			print("Error reading LogData.Section1:", err.Error())
			return false
		}
		log.Section1.Loaded = true
		log.Section2.Loaded = false
		log.Section3.Loaded = false
	case data.Register == 40 && header.PacketLength == 111:
		err = binary.Read(reader, binary.LittleEndian, &log.Raw.Section2)
		if err != nil {
			print("Error reading LogData.Section2:", err.Error())
			return false
		}
		log.Section1.Loaded = false
		log.Section2.Loaded = true
		log.Section3.Loaded = false
	case data.Register == 80 && header.PacketLength == 111:
		err = binary.Read(reader, binary.LittleEndian, &log.Raw.Section3)
		if err != nil {
			print("Error reading LogData.Section3:", err.Error())
			return false
		}
		log.Section1.Loaded = false
		log.Section2.Loaded = false
		log.Section3.Loaded = true
	default:
		println("Unhandled register:", data.Register)
		return false
	}

	log.Scale()
	fmt.Print(log)
	return true
}

func (log *LogData) Scale() {
	log.Section1.Status = log.Raw.Section1.Status
	log.Section1.PV1_Voltage = float32(log.Raw.Section1.PV1_Voltage) / 10
	log.Section1.PV2_Voltage = float32(log.Raw.Section1.PV2_Voltage) / 10
	log.Section1.PV3_Voltage = float32(log.Raw.Section1.PV3_Voltage) / 10
	log.Section1.Battery_Voltage = float32(log.Raw.Section1.Battery_Voltage) / 10
	log.Section1.SOC = float32(log.Raw.Section1.SOC)
	log.Section1.SOH = float32(log.Raw.Section1.SOH)
	log.Section1.PV1_Power = float32(log.Raw.Section1.PV1_Power)
	log.Section1.PV2_Power = float32(log.Raw.Section1.PV2_Power)
	log.Section1.PV3_Power = float32(log.Raw.Section1.PV3_Power)
	log.Section1.Charge_Power = float32(log.Raw.Section1.Charge_Power)
	log.Section1.Discharge_Power = float32(log.Raw.Section1.Discharge_Power)
	log.Section1.Voltage_AC_R = float32(log.Raw.Section1.Voltage_AC_R) / 10
	log.Section1.Voltage_AC_S = float32(log.Raw.Section1.Voltage_AC_S) / 10
	log.Section1.Voltage_AC_T = float32(log.Raw.Section1.Voltage_AC_T) / 10
	log.Section1.Frequency_Grid = float32(log.Raw.Section1.Frequency_Grid) / 100
	log.Section1.ActiveCharge_Power = float32(log.Raw.Section1.ActiveCharge_Power)
	log.Section1.ActiveInverter_Power = float32(log.Raw.Section1.ActiveInverter_Power)
	log.Section1.Inductor_Current = float32(log.Raw.Section1.Inductor_Current) / 100
	log.Section1.Grid_Power_Factor = float32(log.Raw.Section1.Grid_Power_Factor) / 1000
	log.Section1.Voltage_EPS_R = float32(log.Raw.Section1.Voltage_EPS_R) / 10
	log.Section1.Voltage_EPS_S = float32(log.Raw.Section1.Voltage_EPS_S) / 10
	log.Section1.Voltage_EPS_T = float32(log.Raw.Section1.Voltage_EPS_T) / 10
	log.Section1.Frequency_EPS = float32(log.Raw.Section1.Frequency_EPS) / 100
	log.Section1.Active_EPS_Power = float32(log.Raw.Section1.Active_EPS_Power)
	log.Section1.Apparent_EPS_Power = float32(log.Raw.Section1.Apparent_EPS_Power)
	log.Section1.Power_To_Grid = float32(log.Raw.Section1.Power_To_Grid)
	log.Section1.Power_From_Grid = float32(log.Raw.Section1.Power_From_Grid)
	log.Section1.PV1_Energy_Today = float32(log.Raw.Section1.PV1_Energy_Today) / 10
	log.Section1.PV2_Energy_Today = float32(log.Raw.Section1.PV2_Energy_Today) / 10
	log.Section1.PV3_Energy_Today = float32(log.Raw.Section1.PV3_Energy_Today) / 10
	log.Section1.ActiveInverter_Energy_Today = float32(log.Raw.Section1.ActiveInverter_Energy_Today) / 10
	log.Section1.AC_Charging_Today = float32(log.Raw.Section1.AC_Charging_Today) / 10
	log.Section1.Charging_Today = float32(log.Raw.Section1.Charging_Today) / 10
	log.Section1.Discharging_Today = float32(log.Raw.Section1.Discharging_Today) / 10
	log.Section1.EPS_Today = float32(log.Raw.Section1.EPS_Today) / 10
	log.Section1.Exported_Today = float32(log.Raw.Section1.Exported_Today) / 10
	log.Section1.Grid_Today = float32(log.Raw.Section1.Grid_Today) / 10
	log.Section1.Bus1_Voltage = float32(log.Raw.Section1.Bus1_Voltage)
	log.Section1.Bus2_Voltage = float32(log.Raw.Section1.Bus2_Voltage)

	log.Section2.PV1_Energy_Total = float32(log.Raw.Section2.PV1_Energy_Total) / 10
	log.Section2.PV2_Energy_Total = float32(log.Raw.Section2.PV2_Energy_Total) / 10
	log.Section2.PV3_Energy_Total = float32(log.Raw.Section2.PV3_Energy_Total) / 10
	log.Section2.ActiveInverter_Energy_Total = float32(log.Raw.Section2.ActiveInverter_Energy_Total) / 10
	log.Section2.AC_Charging_Total = float32(log.Raw.Section2.AC_Charging_Total) / 10
	log.Section2.Charging_Total = float32(log.Raw.Section2.Charging_Total) / 10
	log.Section2.Discharging_Total = float32(log.Raw.Section2.Discharging_Total) / 10
	log.Section2.EPS_Total = float32(log.Raw.Section2.EPS_Total) / 10
	log.Section2.Exported_Total = float32(log.Raw.Section2.Exported_Total) / 10
	log.Section2.Grid_Total = float32(log.Raw.Section2.Grid_Total) / 10
	log.Section2.FaultCode = log.Raw.Section2.FaultCode
	log.Section2.WarningCode = log.Raw.Section2.WarningCode
	log.Section2.Inner_Temperature = float32(log.Raw.Section2.Inner_Temperature)
	log.Section2.Radiator1_Temperature = float32(log.Raw.Section2.Radiator1_Temperature)
	log.Section2.Radiator2_Temperature = float32(log.Raw.Section2.Radiator2_Temperature)
	log.Section2.Battery_Temperature = float32(log.Raw.Section2.Battery_Temperature)
	log.Section2.Runtime = log.Raw.Section2.Runtime

	log.Section3.BatteryComType = log.Raw.Section3.BatteryComType
	log.Section3.BMS_Max_Charge_Current = float32(log.Raw.Section3.BMS_Max_Charge_Current) / 100
	log.Section3.BMS_Max_Discharge_Current = float32(log.Raw.Section3.BMS_Max_Discharge_Current) / 100
	log.Section3.BMS_Charge_Voltage_Reference = float32(log.Raw.Section3.BMS_Charge_Voltage_Reference) / 10
	log.Section3.BMS_Discharge_Cutoff = float32(log.Raw.Section3.BMS_Discharge_Cutoff) / 10
	log.Section3.BMS_Status = log.Raw.Section3.BMS_Status
	log.Section3.BMS_Inverter_Status = log.Raw.Section3.BMS_Inverter_Status
	log.Section3.Battery_Parallel_Count = log.Raw.Section3.Battery_Parallel_Count
	log.Section3.Battery_Capacity = float32(log.Raw.Section3.Battery_Capacity)
	log.Section3.Battery_Current = float32(log.Raw.Section3.Battery_Current) / 100
	log.Section3.BMS_Event1 = log.Raw.Section3.BMS_Event1
	log.Section3.BMS_Event2 = log.Raw.Section3.BMS_Event2
	log.Section3.MaxCell_Voltage = float32(log.Raw.Section3.MaxCell_Voltage) / 10
	log.Section3.MinCell_Voltage = float32(log.Raw.Section3.MinCell_Voltage) / 10
	log.Section3.MaxCell_Temp = float32(log.Raw.Section3.MaxCell_Temp)
	log.Section3.MinCell_Temp = float32(log.Raw.Section3.MinCell_Temp)
	log.Section3.BMS_FW_Update_State = log.Raw.Section3.BMS_FW_Update_State
	log.Section3.Cycle_Count = log.Raw.Section3.Cycle_Count
	log.Section3.BatteryInverter_Voltage = float32(log.Raw.Section3.BatteryInverter_Voltage) / 10
}

func process(frame []byte, length uint16) {
	log := LogData{}
	if log.Decode(frame, length) {
		fmt.Print(log)
	}
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
		go process(received[:numRead], uint16(numRead))
	}
}
