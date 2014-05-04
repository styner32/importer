package importer

import "testing"

func TestPluralize(t *testing.T) {
	input := "channel"
	result := Pluralize(input)
	expected := "channels"

	if expected != result {
		t.Fatalf("Expected %s, got %s in %s", expected, result)
	}
}

func TestTitleizeOneWord(t *testing.T) {
	input := "channel"
	result := Titleize(input)
	expected := "Channel"

	if expected != result {
		t.Fatalf("Expected %s, got %s in %s", expected, result)
	}
}

func TestToIdColumn(t *testing.T) {
	input := "media_resource"
	result := ToIdColumn(input)
	expected := "media_resource_id"

	if expected != result {
		t.Fatalf("Expected %s, got %s in %s", expected, result)
	}
}

func TestToIdColumn(t *testing.T) {
	input := "language"
	result := ToCodeColumn(input)
	expected := "language_code"

	if expected != result {
		t.Fatalf("Expected %s, got %s in %s", expected, result)
	}
}

func TestUint8ToString(t *testing.T) {
	input := []uint8{104, 101, 108, 108, 111}
	result := Uint8ToString(input)
	expected := "hello"

	if expected != result {
		t.Fatalf("Expected %s, got %s in %s", expected, result)
	}
}
