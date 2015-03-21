package glacier

import (
	"strconv"
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

func TestDataRetrievalPolicy(t *testing.T) {
	conn := testConnection(t)

	originalPolicy, originalBytes, err := conn.GetDataRetrievalPolicy()
	if err != nil {
		t.Fatal("Could not get data retrievel policy.", err)
	}

	// Change the policy.
	var changePolicyTo DataRetrievalPolicy
	var changeBytesTo int
	if originalPolicy == BytesPerHour {
		changePolicyTo = FreeTier
	} else {
		changePolicyTo = BytesPerHour
		changeBytesTo = 1
	}
	err = conn.SetRetrievalPolicy(changePolicyTo, changeBytesTo)
	if err != nil {
		t.Fatal("Could not set data retrieval policy.", err)
	}

	// Verify the policy changed.
	changedPolicy, changedBytes, err := conn.GetDataRetrievalPolicy()
	if err != nil {
		t.Error("Could not get changed data retrieval policy.", err)
	}
	if changedPolicy != changePolicyTo || changedBytes != changeBytesTo {
		t.Error("Change policy did not take effect (instantly).")
	}

	// Reset the original policy.
	err = conn.SetRetrievalPolicy(originalPolicy, originalBytes)
	if err != nil {
		policy := originalPolicy.String()
		if originalPolicy == BytesPerHour {
			policy += " at " + strconv.Itoa(originalBytes)
		}
		t.Fatalf("WARNING!!! Could not reset policy to %s, please reset manually. %v", policy, err)
	}
}
