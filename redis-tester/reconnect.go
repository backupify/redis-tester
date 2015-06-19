package tester

import (
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
)

type ReconnectEvents struct {
	Address     *net.TCPAddr
	Rate        time.Duration
	Shutdown    chan struct{}
	ErrorEvents chan error
}

func (t ReconnectEvents) Stress() {
	timer := time.NewTicker(t.Rate)
	reconnect := func(addr *net.TCPAddr) {
		conn, err := redis.DialTimeout("tcp", t.Address.String(), 50*time.Millisecond, 10*time.Second, 10*time.Second)
		if err != nil {
			t.ErrorEvents <- err
		} else {
			time.Sleep(100 * time.Millisecond)
			conn.Close()
		}
	}

	for {
		select {
		case <-timer.C:
			go reconnect(t.Address)
		case <-t.Shutdown:
			timer.Stop()
			return
		}
	}
}
