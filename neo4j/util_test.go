package neo4j

import (
	"testing"
	"testing/quick"
)

func TestConcatenateStringSlices(t *testing.T) {
	f := func(slices [][]string) bool {
		concatenation := ConcatenateStringSlices(slices...)
		totalLen := 0
		for _, s := range slices {
			totalLen += len(s)
		}
		if totalLen != len(concatenation) {
			return false
		}
		i := 0
		for _, slice := range slices {
			for _, str := range slice {
				if str != concatenation[i] {
					return false
				}
				i += 1
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
