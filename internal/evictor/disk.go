package evictor

import "golang.org/x/sys/unix"

// StatfsUsage reports usage as df does: total is used plus the space
// available to unprivileged users.
func StatfsUsage(dir string) (used, total int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	bs := int64(st.Bsize)
	used = int64(st.Blocks-st.Bfree) * bs
	return used, used + int64(st.Bavail)*bs, nil
}
