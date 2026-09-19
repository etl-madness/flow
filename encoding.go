package flow

import (
	"regexp"
	"unicode/utf16"
)

var utf16DeclRegex = regexp.MustCompile(`(?i)(<\?xml[^>]*\bencoding\s*=\s*)["']utf-16(?:le|be)?["']`)

// NormalizeXMLBytes inspects the byte slice for UTF-8/UTF-16 BOMs or UTF-16 byte patterns,
// transcodes the XML payload to valid UTF-8, and updates any UTF-16 XML declaration to UTF-8.
func NormalizeXMLBytes(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// 1. Strip UTF-8 BOM if present
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:]
	}

	// 2. Detect and transcode UTF-16
	if len(data) >= 2 {
		switch {
		case data[0] == 0xFF && data[1] == 0xFE:
			// UTF-16 LE with BOM
			if len(data)%2 == 0 {
				codeUnits := make([]uint16, 0, len(data)/2-1)
				for i := 2; i+1 < len(data); i += 2 {
					codeUnits = append(codeUnits, uint16(data[i])|uint16(data[i+1])<<8)
				}
				data = []byte(string(utf16.Decode(codeUnits)))
			}
		case data[0] == 0xFE && data[1] == 0xFF:
			// UTF-16 BE with BOM
			if len(data)%2 == 0 {
				codeUnits := make([]uint16, 0, len(data)/2-1)
				for i := 2; i+1 < len(data); i += 2 {
					codeUnits = append(codeUnits, uint16(data[i+1])|uint16(data[i])<<8)
				}
				data = []byte(string(utf16.Decode(codeUnits)))
			}
		default:
			// Detect UTF-16 without BOM by checking for leading '<' (0x003C) after optional whitespace
			if isUTF16LEWithoutBOM(data) {
				codeUnits := make([]uint16, 0, len(data)/2)
				for i := 0; i+1 < len(data); i += 2 {
					codeUnits = append(codeUnits, uint16(data[i])|uint16(data[i+1])<<8)
				}
				data = []byte(string(utf16.Decode(codeUnits)))
			} else if isUTF16BEWithoutBOM(data) {
				codeUnits := make([]uint16, 0, len(data)/2)
				for i := 0; i+1 < len(data); i += 2 {
					codeUnits = append(codeUnits, uint16(data[i+1])|uint16(data[i])<<8)
				}
				data = []byte(string(utf16.Decode(codeUnits)))
			}
		}
	}

	// 3. Rewrite any encoding="UTF-16" declaration in the UTF-8 XML to encoding="UTF-8"
	if utf16DeclRegex.Match(data) {
		data = utf16DeclRegex.ReplaceAll(data, []byte(`${1}"UTF-8"`))
	}

	return data
}

// DecodeTextBytes decodes raw file bytes into a clean UTF-8 string,
// handling UTF-16 LE/BE (with or without BOM) and UTF-8 BOM.
func DecodeTextBytes(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return string(data[3:])
	}

	if len(data) >= 2 {
		if data[0] == 0xFF && data[1] == 0xFE && len(data)%2 == 0 {
			codeUnits := make([]uint16, 0, len(data)/2-1)
			for i := 2; i+1 < len(data); i += 2 {
				codeUnits = append(codeUnits, uint16(data[i])|uint16(data[i+1])<<8)
			}
			return string(utf16.Decode(codeUnits))
		}
		if data[0] == 0xFE && data[1] == 0xFF && len(data)%2 == 0 {
			codeUnits := make([]uint16, 0, len(data)/2-1)
			for i := 2; i+1 < len(data); i += 2 {
				codeUnits = append(codeUnits, uint16(data[i+1])|uint16(data[i])<<8)
			}
			return string(utf16.Decode(codeUnits))
		}

		if isUTF16LEWithoutBOM(data) {
			codeUnits := make([]uint16, 0, len(data)/2)
			for i := 0; i+1 < len(data); i += 2 {
				codeUnits = append(codeUnits, uint16(data[i])|uint16(data[i+1])<<8)
			}
			return string(utf16.Decode(codeUnits))
		}
		if isUTF16BEWithoutBOM(data) {
			codeUnits := make([]uint16, 0, len(data)/2)
			for i := 0; i+1 < len(data); i += 2 {
				codeUnits = append(codeUnits, uint16(data[i+1])|uint16(data[i])<<8)
			}
			return string(utf16.Decode(codeUnits))
		}
	}

	return string(data)
}

func isUTF16LEWithoutBOM(data []byte) bool {
	if len(data) < 2 || len(data)%2 != 0 {
		return false
	}
	// Check first non-whitespace 16-bit code unit
	for i := 0; i+1 < len(data); i += 2 {
		b0, b1 := data[i], data[i+1]
		if b1 == 0x00 {
			switch b0 {
			case ' ', '\t', '\r', '\n':
				continue
			case '<':
				return true
			default:
				return false
			}
		}
		return false
	}
	return false
}

func isUTF16BEWithoutBOM(data []byte) bool {
	if len(data) < 2 || len(data)%2 != 0 {
		return false
	}
	// Check first non-whitespace 16-bit code unit
	for i := 0; i+1 < len(data); i += 2 {
		b0, b1 := data[i], data[i+1]
		if b0 == 0x00 {
			switch b1 {
			case ' ', '\t', '\r', '\n':
				continue
			case '<':
				return true
			default:
				return false
			}
		}
		return false
	}
	return false
}
