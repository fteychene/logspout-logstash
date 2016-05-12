package logstash

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	transport router.AdapterTransport
	route     *router.Route
}

var javaExceptionPattern = regexp.MustCompile("(^.+Exception: .+)|(^\\s+at .+)|(^\\s+... \\d+ more)|(^\\s*Caused by:.+)")
var clojureExceptionPattern = regexp.MustCompile("^.+#error {|^\\s+:cause .+|^\\s+:via$|:type |:message |:at |^\\s+:trace$|^\\s+\\[?\\[.+\\]\\]?}?$")

var lastMessage = make(map[string]LogstashMessage)
var collapseMessage = make(map[string]chan LogstashMessage)

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	for err != nil {
		fmt.Println("Unable to reach " + route.Address + " retryin in 2 sec")
		time.Sleep(time.Second * 2)
		conn, err = transport.Dial(route.Address, route.Options)
	}

	fmt.Println("Successfully connected to " + route.Adapter)
	err = conn.Close()
	if err != nil {
		log.Println("Error closing initial connection", err)
	}

	return &LogstashAdapter{
		route:     route,
		transport: transport,
	}, nil
}

func runCollapseChannel(channel chan LogstashMessage, sendMessage func(message LogstashMessage)) {
	var collapse *LogstashMessage
	timeout := time.After(time.Second * 1)
	for {
		select {
		case log := <-channel:
			switch {
			case log.Type == "log":
				if collapse != nil {
					sendMessage(*collapse)
					collapse = nil
				}
				sendMessage(log)
			case collapse != nil && log.Type == (*collapse).Type:
				(*collapse).Message = strings.Join([]string{(*collapse).Message, log.Message}, "\n")
			case collapse == nil || log.Type != (*collapse).Type:
				if collapse != nil {
					sendMessage(*collapse)
				}
				collapse = &log

			}
			timeout = time.After(time.Second * 1)
		case <-timeout:
			if collapse != nil {
				sendMessage(*collapse)
				collapse = nil
			}
		}
	}
}

func (a *LogstashAdapter) collapseIfNeeded(message LogstashMessage, sendMessage func(message LogstashMessage)) {
	if _, present := collapseMessage[message.Docker.ID]; !present {
		channel := make(chan LogstashMessage)
		collapseMessage[message.Docker.ID] = channel
		go runCollapseChannel(channel, sendMessage)
	}
	switch {
	//  Java exception
	case javaExceptionPattern.MatchString(message.Message):
		message.Type = "java-exception"
	// Clojure exception
	case clojureExceptionPattern.MatchString(message.Message):
		message.Type = "clojure-exception"
	}
	collapseMessage[message.Docker.ID] <- message
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}
		msg := LogstashMessage{
			Message:  m.Data,
			EvenTime: m.Time,
			Type:     "log",
			Docker:   dockerInfo,
		}

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err == nil {
			msg.MessageInfo = jsonMsg
		}

		fmt.Println("Send ", msg.Message)

		a.collapseIfNeeded(msg, func(message LogstashMessage) {
			js, err := json.Marshal(message)
			conn, err := a.transport.Dial(a.route.Address, a.route.Options)
			if err != nil {
				log.Println("logstash:", err)
			}
			_, err = conn.Write(js)
			if err != nil {
				log.Println("logstash:", err)
			}
			err = conn.Close()
			if err != nil {
				log.Println("logstash:", err)
			}
		})
	}
}

// DockerInfo : informations embbeded in message to logstash
type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash
type LogstashMessage struct {
	Message     string                 `json:"message"`
	EvenTime    time.Time              `json:"eventTime"`
	MessageInfo map[string]interface{} `json:"messageInfo"`
	Docker      DockerInfo             `json:"docker"`
	Type        string                 `json:"type"`
}
