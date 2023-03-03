package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashRanges(t *testing.T) {
	verify := func(totalNodes int, expected [][]int) {
		for i := 0; i < totalNodes; i++ {
			actual, err := hashRanges(i, totalNodes)
			require.NoError(t, err)
			require.Equalf(t, expected[i], actual, "unexpected hash ranges for index %d of total %d", i, totalNodes)
		}
	}

	verify(1, [][]int{{0, 65535}})
	verify(2, [][]int{{0, 32767}, {32768, 65535}})
	verify(3, [][]int{{0, 21844}, {21845, 43689}, {43690, 65535}})
	verify(4, [][]int{{0, 16383}, {16384, 32767}, {32768, 49151}, {49152, 65535}})
	verify(5, [][]int{{0, 13106}, {13107, 26213}, {26214, 39320}, {39321, 52427}, {52428, 65535}})
}
