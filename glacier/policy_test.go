package glacier

import (
	"testing"
)

func TestToDataRetrievalPolicy(t *testing.T) {
	cases := []struct {
		input string
		want  DataRetrievalPolicy
	}{
		{"BytesPerHour", BytesPerHour},
		{"FreeTier", FreeTier},
		{"None", None},
		{" bYTES pER hOUR ", BytesPerHour},
		{"a", InvalidDataRetrievalPolicy},
	}

	var zero DataRetrievalPolicy
	if zero != InvalidDataRetrievalPolicy {
		t.Error("zero value is not InvalidDataRetrievalPolicy")
	}

	for _, v := range cases {
		got := ToDataRetrievalPolicy(v.input)
		if got != v.want {
			t.Errorf("%q; want %v, got %v", v.input, v.want, got)
		}
	}
}

func BenchmarkToDataRetrievalPolicyFastPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToDataRetrievalPolicy("BytesPerHour")
	}
}

func BenchmarkToDataRetrievalPolicySlowPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToDataRetrievalPolicy(" bYTES pER hOUR ")
	}
}
