package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/backupify/redis-tester"
	"github.com/garyburd/redigo/redis"
	"gopkg.in/alecthomas/kingpin.v1"
)

const (
	operations  string = "operations"  // crud operations
	connections string = "connections" // opening and closing tcp connections
)

func Destress(shutdown chan struct{}) {
	shutdown <- struct{}{}
}

type configuration struct {
	TCPAddr       *net.TCPAddr
	StresserCount int
	StresserMode  *string
	Metrics       struct {
		Enabled bool
	}
	Autoscale struct {
		Enabled bool
		Rate    time.Duration
	}
	ReconnectEvents struct {
		// Enabled bool          // enable open and closing connections.
		Rate time.Duration // rate to generate the events in milliseconds.
		// Ratio float64       // percent of connections (based on PoolConfiguration.MaxActive) to close on each event.
	}
	PoolConfiguration struct {
		MaxActive   int
		MaxIdle     int
		IdleTimeout time.Duration
	}
}

type stress interface {
	Stress()
}

type application struct {
	stresser    stress
	slowdown    chan struct{} // channel for sending stop requests to stressers
	errorEvents chan error    // channel for emitting error events from stressers.
}

func (t application) Increase() {
	go t.stresser.Stress()
}

func (t application) Decrease() {
	t.slowdown <- struct{}{}
}

func newOperationsStress(conf configuration, monitor tester.Monitor, shutdown chan struct{}, errorEvents chan error) stress {
	return tester.Stresser{
		Monitor:  monitor,
		Shutdown: shutdown,
		Pool: &redis.Pool{
			MaxIdle:     conf.PoolConfiguration.MaxIdle,
			MaxActive:   conf.PoolConfiguration.MaxActive,
			IdleTimeout: conf.PoolConfiguration.IdleTimeout,
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				var err error
				log.Println("opening connection")
				if _, err = c.Do("PING"); err != nil {
					log.Println("Failed to connect to redis", conf.TCPAddr.String(), err)
				}
				return err
			},
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", conf.TCPAddr.String())
			},
		},
		ErrorEvents: errorEvents,
	}
}

func newReconnectStress(conf configuration, shutdown chan struct{}, errorEvents chan error) stress {
	return tester.ReconnectEvents{
		Address:     conf.TCPAddr,
		Rate:        conf.ReconnectEvents.Rate,
		Shutdown:    shutdown,
		ErrorEvents: errorEvents,
	}
}

func newApplication(stresser stress, shutdown chan struct{}, errorEvents chan error) application {
	return application{
		stresser:    stresser,
		slowdown:    shutdown,
		errorEvents: errorEvents,
	}
}

func main() {
	defer log.Println("Shutting down")
	runtime.GOMAXPROCS(runtime.NumCPU())

	var conf configuration
	var stresser stress

	conf.StresserMode = new(string)
	shutdown := make(chan struct{}, 15)
	errorEvents := make(chan error, 1000)
	monitor := tester.NewMonitor()

	log.Println("PID", os.Getpid())

	app := kingpin.New("spike", "spike command line for testing functionality")
	app.Flag("stresser-count", "number of concurrent stressers to immediately start").Default("0").IntVar(&conf.StresserCount)
	app.Flag("stresser-mode", "what kind of stress load you want to generate").Default("operations").EnumVar(&conf.StresserMode, operations, connections)
	app.Flag("address", "tcp address of the redis server").Default("localhost:6379").TCPVar(&conf.TCPAddr)
	app.Flag("metrics-enabled", "enable logging metrics").Default("false").BoolVar(&conf.Metrics.Enabled)
	app.Flag("pool-active-max", "maximum number of connections to allow").Default("0").IntVar(&conf.PoolConfiguration.MaxActive)
	app.Flag("pool-idle-max", "maximum number of idle connections to allow").Default("5").IntVar(&conf.PoolConfiguration.MaxIdle)
	app.Flag("pool-idle-timeout", "how long before closing an unused idle connection").Default("5s").DurationVar(&conf.PoolConfiguration.IdleTimeout)
	app.Flag("auto-scale-enabled", "continuously increase the stress until errors occur").Default("false").BoolVar(&conf.Autoscale.Enabled)
	app.Flag("auto-scale-rate", "time between increases in stress").Default("50ms").DurationVar(&conf.Autoscale.Rate)
	app.Flag("reconnect-events-rate", "time between reopen a connection").Default("2s").DurationVar(&conf.ReconnectEvents.Rate)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	switch *conf.StresserMode {
	case operations:
		log.Println("running operations stress mode")
		stresser = newOperationsStress(conf, monitor, shutdown, errorEvents)
	case connections:
		log.Println("running connections stress mode")
		stresser = newReconnectStress(conf, shutdown, errorEvents)
	default:
		log.Fatalln("unknown stress mode")
	}

	application := newApplication(stresser, shutdown, errorEvents)

	auto := autoscale{app: application, resume: make(chan struct{}), rate: conf.Autoscale.Rate}

	if conf.Autoscale.Enabled {
		go auto.AutoScale()
	}

	if conf.Metrics.Enabled {
		go PrintMonitor(conf.TCPAddr.String(), monitor)
	}

	for i := 0; i < conf.StresserCount; i++ {
		application.Increase()
	}

	log.Println(conf.TCPAddr, "Stresser Count Reached")
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGKILL, syscall.SIGINT)

	for {
		sig := <-sigc
		log.Println("Signal Recieved", sig)
		switch sig {
		case syscall.SIGINT, syscall.SIGKILL:
			return
		case syscall.SIGUSR1:
			auto.resume <- struct{}{}
		case syscall.SIGUSR2:
			application.Decrease()
		default:
			log.Println("unhandled signal")
		}
	}
}

type reconnectEvents struct {
	tester.ReconnectEvents
}

func (t reconnectEvents) Increase() {
	go t.ReconnectEvents.Stress()
}

func (t reconnectEvents) Decrease() {
	t.ReconnectEvents.Shutdown <- struct{}{}
}

type autoscale struct {
	app         application
	scalePeriod time.Duration
	resume      chan struct{}
	rate        time.Duration
}

// automatically scale up until errors occur
func (t autoscale) AutoScale() {
	timer := time.NewTicker(t.rate)

	for {
		select {
		case event := <-t.app.errorEvents:
			// error event occurred disable
			log.Println("Error Event:", event)
			timer.Stop()
		case <-timer.C:
			log.Println("Autoscaling Stress")
			t.app.Increase()
		case <-t.resume:
			timer = time.NewTicker(t.rate)
		}
	}
}

func PrintMonitor(name string, m tester.Monitor) {
	t := time.Tick(5 * time.Second)
	for {
		<-t
		results := m.Metrics()
		log.Printf("%s Read: %d Write: %d Delete: %d Overwrite: %d Total: %d\n", name, results.Reads, results.Writes, results.Deletes, results.Overwrites, results.Total)
	}
}
