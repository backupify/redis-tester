package tester

import (
	crand "crypto/rand"
	"encoding/hex"
	"log"
	"math/rand"

	"github.com/garyburd/redigo/redis"
)

func GenerateValue(length int) string {
	buffer := make([]byte, length)
	if _, err := crand.Read(buffer); err != nil {
		panic(err) // should never happen?
	}

	return hex.EncodeToString(buffer)
}

func RandomLengthValue(maxLength int) string {
	return GenerateValue(rand.Intn(maxLength))
}

type Stresser struct {
	Pool *redis.Pool
	Monitor
	Shutdown    chan struct{}
	ErrorEvents chan error
}

func (t Stresser) Stress() {
	conn := t.Pool.Get()
	defer conn.Close()

	if conn.Err() != nil {
		t.ErrorEvents <- conn.Err()
	}

	if _, err := conn.Do("PING"); err != nil {
		t.ErrorEvents <- err
	}

	for {
		select {
		case <-t.Shutdown:
			log.Println("Shutdown Recieved")
			return
		default:
			if err := t.PerformOperation(conn); err != nil {
				t.ErrorEvents <- err
				return
			}
		}
	}
}

func (t Stresser) PerformOperation(conn redis.Conn) error {
	x := rand.Intn(100)

	switch {
	case x < 25:
		return t.ReadOperation(conn)
	case x > 25 && x < 50:
		return t.WriteOperation(conn)
	case x > 50 && x < 75:
		return t.OverwriteOperation(conn)
	default:
		return t.DeleteOperation(conn)
	}
}

func (t Stresser) ReadOperation(conn redis.Conn) error {
	key, err := conn.Do("RANDOMKEY")
	// Ignore nil errors
	if err == redis.ErrNil {
		return nil
	}

	if err != nil {
		log.Println("Read: Failed Retrieving Random Key", err)
		return err
	}

	_, err = redis.String(conn.Do("GET", key))

	if err != nil && err != redis.ErrNil {
		log.Println("Read: Failed Reading Key", err)
		return err
	}

	// Only record successful operations
	t.Monitor.ReadOp()
	return nil
}

func (t Stresser) WriteOperation(conn redis.Conn) error {
	if _, err := conn.Do("SET", GenerateValue(10), RandomLengthValue(1024)); err != nil {
		log.Println("Write: Failed Writing Key", err)
		return err
	}

	// Only record successful operations
	t.Monitor.WriteOp()
	return nil
}

func (t Stresser) OverwriteOperation(conn redis.Conn) error {
	key, err := redis.String(conn.Do("RANDOMKEY"))
	// Ignore nil errors
	if err == redis.ErrNil {
		return nil
	}

	if err != nil {
		log.Println("Overwrite: Failed Retrieving Random Key", err)
		return err
	}

	if _, err := conn.Do("SET", key, RandomLengthValue(1024)); err != nil {
		log.Println("Overwrite: Failed Overwriting Key", err)
		return err
	}

	// Only record successful operations
	t.Monitor.OverwriteOp()
	return nil
}

func (t Stresser) DeleteOperation(conn redis.Conn) error {
	key, err := redis.String(conn.Do("RANDOMKEY"))
	// Ignore nil errors
	if err == redis.ErrNil {
		return nil
	}

	if err != nil {
		log.Println("Delete: Failed Retrieving Random Key", err)
		return err
	}

	if _, err := conn.Do("DEL", key); err != nil {
		log.Println("Delete: Failed Overwriting Key", err)
		return err
	}

	// Only record successful operations
	t.Monitor.DeleteOp()
	return nil
}
