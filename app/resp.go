package main

import (
	"fmt"
	"strconv"
)

// Type of RESP
type Type byte

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

type RESP struct {
	Type  Type
	Raw   []byte
	Data  []byte
	Count int
}

type Command struct {
	name string
	args []string
}

func ReadNextRESP(b []byte) (n int, resp RESP) {
	if len(b) == 0 {
		return 0, RESP{} // no data to read
	}

	resp.Type = Type(b[0])
	switch resp.Type {
	case Integer, String, Bulk, Array, Error:
	default:
		return 0, RESP{} // invalid kind
	}

	// read to end of line
	i := 1
	for ; ; i++ {
		if i == len(b) {
			return 0, RESP{} // not enough data
		}
		if b[i] == '\n' {
			if b[i-1] != '\r' {
				return 0, RESP{} //, missing CR character
			}
			i++
			break
		}
	}

	resp.Raw = b[0:i]
	resp.Data = b[1 : i-2]

	switch resp.Type {
	case Integer:
		if len(resp.Data) == 0 {
			return 0, RESP{} //, invalid integer
		}
		var j int
		if resp.Data[0] == '-' {
			if len(resp.Data) == 1 {
				return 0, RESP{} //, invalid integer
			}
			j++
		}
		for ; j < len(resp.Data); j++ {
			if resp.Data[j] < '0' || resp.Data[j] > '9' {
				return 0, RESP{} // invalid integer
			}
		}
		return len(resp.Raw), resp
	case String:
	case Error:
		return len(resp.Raw), resp
	}

	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data))
	if err != nil {
		return 0, RESP{} // invalid number of bytes
	}

	switch resp.Type {
	case Bulk:
		n, _ := strconv.Atoi(string(b[1 : i-2]))
		resp.Data = b[i : i+n]
		return i + n + 2, resp
	}

	resp.Data = b[1 : i-2]

	return i, resp
}

func ReadNextCommand(data []byte) *Command {
	readBytes := 0
	consumedBytes, commandHeader := ReadNextRESP(data)
	if commandHeader.Type != Array {
		return nil
	}

	readBytes += consumedBytes
	resps := []RESP{}
	for readBytes < len(data) {
		consumed, resp := ReadNextRESP(data[readBytes:])
		if consumed == 0 {
			break
		}
		readBytes += consumed
		resps = append(resps, resp)
	}

	args := make([]string, len(resps)-1)
	for i, v := range resps[1:] {
		args[i] = string(v.Data)
	}

	command := Command{
		name: string(resps[0].Data),
		args: args,
	}

	return &command
}

func ToBulkString(data string) []byte {
	len := len(data)
	bulkString := fmt.Sprintf("$%d\r\n%s\r\n", len, data)
	return []byte(bulkString)
}
