#!/usr/bin/env bash
# build-release.sh builds ChronicleDB release artifacts for every
# platform in docs/support-matrix.md, deterministically named and
# checksummed, exactly as .github/workflows/release.yml does for a
# tagged release (docs/releasing.md). It never touches anything
# outside its own output directory (default: dist/, already
# gitignored).
#
# Usage:
#   scripts/build-release.sh <version> [output-dir]
#
# <version> should look like v0.1.0 (docs/versioning.md); it is
# embedded into the binary via -ldflags and used in every artifact's
# filename verbatim.
set -euo pipefail

if [[ $# -lt 1 || -z "$1" ]]; then
	echo "usage: $0 <version> [output-dir]   (e.g. $0 v0.1.0)" >&2
	exit 1
fi
VERSION="$1"
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	echo "warning: version '$VERSION' does not look like vMAJOR.MINOR.PATCH (docs/versioning.md); continuing anyway" >&2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${2:-$ROOT_DIR/dist}"
[[ "$OUT_DIR" != /* ]] && OUT_DIR="$PWD/$OUT_DIR"
COMMIT="$(git rev-parse --short=12 HEAD 2>/dev/null || echo none)"
DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

MODULE="github.com/SamudralaAjaykumarrr/chronicledb"
# docs/versioning.md: the version reported by `-version` has no
# leading "v" (tag v0.1.0 -> "chronicledb-node 0.1.0 ..."), even
# though the tag itself, and every archive filename below, keeps it.
VERSION_NUM="${VERSION#v}"
LDFLAGS="-s -w -X ${MODULE}/internal/version.Version=${VERSION_NUM} -X ${MODULE}/internal/version.Commit=${COMMIT} -X ${MODULE}/internal/version.Date=${DATE}"

# GOOS/GOARCH pairs actually declared in docs/support-matrix.md as
# release targets. Keep this list and that document in sync.
TARGETS=(
	"linux amd64"
	"linux arm64"
	"darwin amd64"
	"darwin arm64"
	"windows amd64"
)

echo "building ChronicleDB ${VERSION} (commit ${COMMIT}, ${DATE})"
echo "output directory: ${OUT_DIR}"

# Only ever remove/recreate our own output directory, never anything
# else in the repository or filesystem.
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

WORK_DIR=""
cleanup_work_dir() {
	if [[ -n "$WORK_DIR" ]]; then
		rm -rf "$WORK_DIR"
	fi
}
trap cleanup_work_dir EXIT

for target in "${TARGETS[@]}"; do
	read -r GOOS GOARCH <<<"$target"
	BIN_NAME="chronicledb-node"
	[[ "$GOOS" == "windows" ]] && BIN_NAME="chronicledb-node.exe"
	ARCHIVE_BASE="chronicledb-node_${VERSION}_${GOOS}_${GOARCH}"

	WORK_DIR="$(mktemp -d)"

	echo "  ${GOOS}/${GOARCH}..."
	if ! GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 \
		go build -trimpath -ldflags "$LDFLAGS" -o "${WORK_DIR}/${BIN_NAME}" ./cmd/chronicledb-node; then
		echo "build failed for ${GOOS}/${GOARCH}" >&2
		exit 1
	fi

	if [[ "$GOOS" == "windows" ]]; then
		(cd "$WORK_DIR" && zip -q "${OUT_DIR}/${ARCHIVE_BASE}.zip" "$BIN_NAME")
	else
		tar -czf "${OUT_DIR}/${ARCHIVE_BASE}.tar.gz" -C "$WORK_DIR" "$BIN_NAME"
	fi
	rm -rf "$WORK_DIR"
	WORK_DIR=""
done

(
	cd "$OUT_DIR"
	sha256sum -- *.tar.gz *.zip >checksums.txt
)

echo
echo "release artifacts:"
ls -la "$OUT_DIR"
echo
echo "checksums (dist/checksums.txt):"
cat "$OUT_DIR/checksums.txt"
