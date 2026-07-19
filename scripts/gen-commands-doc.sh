#!/bin/sh
# Regenerates COMMANDS.md from the compiled binary's --help output, so the
# command reference cannot drift from the code. Run via `make docs-commands`.
set -eu

BIN=${1:?usage: gen-commands-doc.sh <path-to-tinvest-binary>}
OUT=COMMANDS.md

subcommands() {
	"$@" --help 2>/dev/null |
		awk '/^Available Commands:/{f=1;next} f&&/^$/{exit} f{print $1}'
}

{
	echo "# tinvest command reference"
	echo
	echo "Generated from the binary's \`--help\` output (\`make docs-commands\`);"
	echo "do not edit by hand. The semantic contract — JSON envelope, exit"
	echo "codes, reconcile protocol, NDJSON streams, guardrails — is in"
	echo "[AGENTS.md](AGENTS.md)."
	echo
	echo '```'
	"$BIN" --help
	echo '```'
	for group in $(subcommands "$BIN"); do
		case "$group" in help | completion) continue ;; esac
		echo
		echo "## tinvest $group"
		echo
		echo '```'
		"$BIN" "$group" --help
		echo '```'
		for sub in $(subcommands "$BIN" "$group"); do
			echo
			echo "### tinvest $group $sub"
			echo
			echo '```'
			"$BIN" "$group" "$sub" --help
			echo '```'
		done
	done
} >"$OUT"

echo "wrote $OUT"
