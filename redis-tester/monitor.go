package tester

import "sync/atomic"

type MonitorResults struct {
	Reads      int64
	Writes     int64
	Deletes    int64
	Overwrites int64
	Total      int64
}

type Monitor interface {
	ReadOp()
	WriteOp()
	OverwriteOp()
	DeleteOp()
	Metrics() MonitorResults
}

type monitor struct {
	reads      int64
	writes     int64
	deletes    int64
	overwrites int64
}

func NewMonitor() Monitor {
	return &monitor{}
}

func (t *monitor) ReadOp() {
	atomic.AddInt64(&t.reads, 1)
}

func (t *monitor) WriteOp() {
	atomic.AddInt64(&t.writes, 1)
}

func (t *monitor) OverwriteOp() {
	atomic.AddInt64(&t.overwrites, 1)
}

func (t *monitor) DeleteOp() {
	atomic.AddInt64(&t.deletes, 1)
}

func (t *monitor) Metrics() MonitorResults {
	return MonitorResults{
		Reads:      t.reads,
		Writes:     t.writes,
		Overwrites: t.overwrites,
		Deletes:    t.deletes,
		Total:      t.reads + t.writes + t.overwrites + t.deletes,
	}
}
