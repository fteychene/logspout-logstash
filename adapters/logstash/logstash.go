package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
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

var collapseMessage map[string]LogstashMessage = make(map[string]LogstashMessage)

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

func (a *LogstashAdapter) TryCollapseMessage(message LogstashMessage, sendMessage func(message LogstashMessage)(int, error)) (int, error) {
	var messageToSend *LogstashMessage = nil
	if strings.Contains(message.Message, "Exception:") {
		if value, present := collapseMessage[message.Docker.ID]; present {
			messageToSend = &value
		}
		collapseMessage[message.Docker.ID] = message
	} else {
		if value, present := collapseMessage[message.Docker.ID]; present {
			message.Message = strings.Join([]string {value.Message, message.Message}, "\n")
			delete(collapseMessage, message.Docker.ID)
		}
		messageToSend = &message
	}
	if messageToSend == nil {
		return 0, nil
	}
	return sendMessage(*messageToSend)
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
		//var js []byte

		msg := LogstashMessage{
			Message: m.Data,
			Docker:  dockerInfo,
		}

		//var jsonMsg map[string]interface{}
		//err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		//if err != nil {
		//	// the message is not in JSON make a new JSON message
		//	jsonMsg = LogstashMessage{
		//		Message: m.Data,
		//		Docker:  dockerInfo,
		//	}
		//	//js, err = json.Marshal(jsonMsg)
		//	//if err != nil {
		//	//	log.Println("logstash:", err)
		//	//	continue
		//	//}
		//} else {
		//	// the message is already in JSON just add the docker specific fields as a nested structure
		//	jsonMsg["docker"] = dockerInfo
		//
		//	//js, err = json.Marshal(jsonMsg)
		//	//if err != nil {
		//	//	log.Println("logstash:", err)
		//	//	continue
		//	//}
		//}
		_, err := a.TryCollapseMessage(msg, func(message LogstashMessage) (int, error) {
			js, err := json.Marshal(message)
			if err != nil {
				return 0, err
			}
			return a.conn.Write(js)

		})
		//_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Docker  DockerInfo `json:"docker"`
}
