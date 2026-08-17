package client

import "io"

// ExtractTarForTest exposes the untar half to the package's external tests.
//
// It is exported for tests only, and specifically so the traversal cases can
// feed it an archive built by hand: no cooperating far end would ever send
// one, and the whole point of the check is that the far end may not be
// cooperating.
func ExtractTarForTest(r io.Reader, destDir, rename string) error {
	return extractTar(r, destDir, rename)
}
