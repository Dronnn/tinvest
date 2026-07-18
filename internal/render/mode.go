package render

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/term"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mode picks the output format: an explicit setting (-o flag, TINVEST_OUTPUT,
// or profile default) wins unconditionally; otherwise table on a TTY, JSON
// everywhere else. Agents must pass an explicit setting rather than rely on
// TTY sniffing (plan §7).
func Mode(explicit string, stdout *os.File) string {
	if explicit != "" {
		return explicit
	}
	if isTTY(stdout) {
		return "table"
	}
	return "json"
}

// isTTY uses a real terminal ioctl: a char-device heuristic would misread
// /dev/null as a TTY.
func isTTY(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}

// Table writes a minimal aligned table with an upper-case header row.
func Table(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	writeRow(tw, headers)
	for _, row := range rows {
		writeRow(tw, row)
	}
	return tw.Flush()
}

func writeRow(w io.Writer, cells []string) {
	_, _ = fmt.Fprintln(w, strings.Join(cells, "\t"))
}

// Timestamp renders a protobuf timestamp as RFC 3339 UTC; zero and nil
// timestamps render as "" (omitted in JSON).
func Timestamp(ts *timestamppb.Timestamp) string {
	if ts == nil || (ts.GetSeconds() == 0 && ts.GetNanos() == 0) {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}
