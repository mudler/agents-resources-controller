package worker

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

var (
	bootIDOnce sync.Once
	bootIDVal  string
)

// BootID returns a value that changes when the machine reboots and is stable
// while it stays up. The controller uses a change as proof that nothing from
// before can still be holding a device; an empty value is treated as no
// proof, which quarantines rather than frees.
func BootID() string {
	bootIDOnce.Do(func() {
		if b, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
			bootIDVal = strings.TrimSpace(string(b))
			return
		}
		// Fallback: the kernel's boot timestamp from /proc/stat.
		f, err := os.Open("/proc/stat")
		if err != nil {
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if rest, ok := strings.CutPrefix(sc.Text(), "btime "); ok {
				bootIDVal = "btime-" + strings.TrimSpace(rest)
				return
			}
		}
	})
	return bootIDVal
}
