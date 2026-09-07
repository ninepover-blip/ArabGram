package metrics

import (
	"math/rand"
	"strings"
	"testing"
)

func TestScrapeSortPreservesConcatenatedKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(3481))
	parts := []string{"", "a", "ab", "b", "bc", "=", ",", "a=b,", "雪", "\x00", "\xff"}
	pick := func() string { return parts[rng.Intn(len(parts))] }
	for range 2000 {
		a := seriesKey{name: pick(), k1: pick(), v1: pick(), k2: pick(), v2: pick(), k3: pick(), v3: pick()}
		b := seriesKey{name: pick(), k1: pick(), v1: pick(), k2: pick(), v2: pick(), k3: pick(), v3: pick()}
		want := strings.Compare(a.name, b.name)
		if want == 0 {
			want = strings.Compare(a.k1+a.v1+a.k2+a.v2+a.k3+a.v3, b.k1+b.v1+b.k2+b.v2+b.k3+b.v3)
		}
		if got := compareSeries(a, b); got != want {
			t.Fatalf("series order got %d want %d: %+v %+v", got, want, a, b)
		}
		left, right := make([]Label, rng.Intn(4)), make([]Label, rng.Intn(4))
		for _, labels := range [][]Label{left, right} {
			for i := range labels {
				labels[i] = Label{Name: pick(), Value: pick()}
			}
		}
		flatten := func(labels []Label) string {
			var out string
			for _, l := range labels {
				out += l.Name + "=" + l.Value + ","
			}
			return out
		}
		if got, want := compareLabels(left, right), strings.Compare(flatten(left), flatten(right)); got != want {
			t.Fatalf("label order got %d want %d: %+v %+v", got, want, left, right)
		}
	}
}

func TestSanitizedMetricNameFastPathBoundaries(t *testing.T) {
	for input, want := range map[string]string{
		"": "telesrv_invalid_metric", "a:b_9": "a:b_9", "9bad": "_bad",
		"雪😃": "__", "a\xff\x00b": "a__b", "a b": "a_b", "_": "_", ":": ":",
	} {
		if got := sanitizeMetricName(input); got != want {
			t.Errorf("sanitize %q got %q want %q", input, got, want)
		}
	}
}
