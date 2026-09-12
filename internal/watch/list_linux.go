//go:build linux && amd64

package watch

// ListQueues returns current, validated TX slots without modifying them.
func ListQueues(target *Target, bpf *BPF) ([]QueueRow, error) {
	fds, _, err := target.Inventory()
	if err != nil {
		return nil, err
	}
	rows := make([]QueueRow, 0, len(fds))
	for _, fd := range fds {
		_, row, err := liveRow(target, bpf, fd)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
