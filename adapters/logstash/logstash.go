package logstash

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
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
	conn  net.Conn
	route *router.Route
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
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

func runCollapseChannel(channel chan LogstashMessage, message LogstashMessage, sendMessage func(message LogstashMessage)) {
	data := message
	finish := false
	for finish == false {
		select {
		case followingMessage, more := <-channel:
			if more {
				if followingMessage.Type == data.Type {
					data.Message = strings.Join([]string{data.Message, followingMessage.Message}, "\n")
				} else {
					sendMessage(data)
					data = followingMessage
				}
			} else {
				sendMessage(data)
				delete(collapseMessage, message.Docker.ID)
				finish = true
			}
		case <-time.After(time.Millisecond * 200):
			fmt.Println("Warning : Collapsing timeout before ending, some part of the message could be lost")
			close(channel)
		}
	}
}

func (a *LogstashAdapter) collapseIfNeeded(message LogstashMessage, sendMessage func(message LogstashMessage)) {
	if javaExceptionPattern.MatchString(message.Message) {
		message.Type = "java-exception"
		if _, present := collapseMessage[message.Docker.ID]; !present {
			channel := make(chan LogstashMessage)
			collapseMessage[message.Docker.ID] = channel
			go runCollapseChannel(channel, message, sendMessage)
		} else {
			collapseMessage[message.Docker.ID] <- message
		}
	} else if clojureExceptionPattern.MatchString(message.Message) {
		message.Type = "clojure-exception"
		if _, present := collapseMessage[message.Docker.ID]; !present {
			channel := make(chan LogstashMessage)
			collapseMessage[message.Docker.ID] = channel
			go runCollapseChannel(channel, message, sendMessage)
		} else {
			collapseMessage[message.Docker.ID] <- message
		}
	} else {
		if _, present := collapseMessage[message.Docker.ID]; present {
			close(collapseMessage[message.Docker.ID])
		}
		sendMessage(message)
	}
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
			Message: m.Data,
			Time:    m.Time,
			Type:    "log",
			Docker:  dockerInfo,
		}

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err == nil {
			msg.MessageInfo = jsonMsg
		}

		fmt.Println("Send ", msg.Message)

		a.collapseIfNeeded(msg, func(message LogstashMessage) {
			js, err := json.Marshal(message)
			if err != nil {
				log.Println("logstash:", err)
			}
			_, err = a.conn.Write(js)
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
	Time        time.Time              `json:"time"`
	MessageInfo map[string]interface{} `json:"messageInfo"`
	Docker      DockerInfo             `json:"docker"`
	Type        string                 `json:"type"`
}
