package importer

import (
	"bytes"
	"strings"
)

func Pluralize(str string) string {
	var buffer bytes.Buffer
	buffer.WriteString(str)
	buffer.WriteString("s")
	return buffer.String()
}

func Titleize(str string) string {
	var buffer bytes.Buffer
	words := strings.Split(str, "_")

	for _, word := range words {
		buffer.WriteString(strings.ToUpper(string(word[0])))
		buffer.WriteString(word[1:])
	}
	return buffer.String()
}

func ToIdColumn(str string) string {
	var buffer bytes.Buffer
	buffer.WriteString(str)
	buffer.WriteString("_id")
	return buffer.String()
}

func ToCodeColumn(str string) string {
	var buffer bytes.Buffer
	buffer.WriteString(str)
	buffer.WriteString("_code")
	return buffer.String()
}

func Uint8ToString(chrs []uint8) string {
	var buffer bytes.Buffer

	for _, chr := range chrs {
		buffer.WriteByte(byte(chr))
	}
	return buffer.String()
}
