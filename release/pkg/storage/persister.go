package storage

import (
	"os"
	"path/filepath"
	"sync"
)

type FilePersister struct {
	mu        sync.Mutex
	dir       string
	raftState []byte
	snapshot  []byte
}

func MakeFilePersister(dir string) *FilePersister {
	_ = os.MkdirAll(dir, 0755)
	p := &FilePersister{
		dir: dir,
	}
	p.raftState, _ = os.ReadFile(filepath.Join(dir, "raftstate"))
	p.snapshot, _ = os.ReadFile(filepath.Join(dir, "snapshot"))
	return p
}

func (p *FilePersister) Save(raftstate []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = raftstate
	p.snapshot = snapshot

	_ = os.WriteFile(filepath.Join(p.dir, "raftstate"), raftstate, 0644)
	if snapshot != nil {
		_ = os.WriteFile(filepath.Join(p.dir, "snapshot"), snapshot, 0644)
	}
}

func (p *FilePersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftState
}

func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}

func (p *FilePersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func (p *FilePersister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}
