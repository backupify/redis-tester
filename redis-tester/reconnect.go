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
	stopper := make(chan struct{})
	timer := time.NewTicker(t.Rate)

	// LIFO order
	defer close(stopper) // second cleanup stopper
	defer timer.Stop()   // first stop the timer

	reconnect := func(addr *net.TCPAddr) {
		conn, err := redis.DialTimeout("tcp", t.Address.String(), 50*time.Millisecond, 10*time.Second, 10*time.Second)
		if err != nil {
			t.ErrorEvents <- err
			stopper <- struct{}{}
		} else {
			conn.Do("PING")
			conn.Close()
		}
	}

	for {
		select {
		case <-timer.C:
			reconnect(t.Address)
		case <-t.Shutdown:
			return
		case <-stopper:
			return
		}
	}

}
