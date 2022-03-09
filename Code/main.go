package main

import (
	"fmt"
	"io/ioutil"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influx "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v2"
)

var conf Config
var topic_list []string = []string{}

type Config struct {
	Broker struct {
		IP   string `yaml:"ip"`
		Port int    `yaml:"port"`
	} `yaml:"Broker"`
	Influx struct {
		IP     string `yaml:"ip"`
		Port   int    `yaml:"port"`
		Bucket string `yaml:"bucket"`
		Org    string `yaml:"org"`
		Token  string `yaml:"token"`
	} `yaml:"Influx"`
	Topic []struct {
		Topic string `yaml:"topic"`
		Parse []struct {
			Tag   string `yaml:"tag"`
			Field string `yaml:"field"`
		} `yaml:"parse"`
	} `yaml:"Topic"`
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	//fmt.Printf("Received message: %s from topic: %v\n", msg.Payload(), msg.Topic())
	pars_topic(msg.Topic(), msg.Payload())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	log.Info("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Infof("Connect lost: %v", err)
}

func initLogger() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.ForceColors = true
	customFormatter.FullTimestamp = true
	log.SetLevel(log.DebugLevel)

}
func pars_topic(topic string, payload []byte) {
	server := fmt.Sprintf("http://%s:%v", conf.Influx.IP, conf.Influx.Port)
	client := influx.NewClient(server, conf.Influx.Token)
	writeAPI := client.WriteAPI(conf.Influx.Org, conf.Influx.Bucket)
	// always close client at the end
	defer client.Close()
	for i := 0; i < len(topic_list); i++ {
		if topic == topic_list[i] {
			for j := 0; j < len(conf.Topic[i].Parse); j++ {

				tag := gjson.Get(string(payload), conf.Topic[i].Parse[j].Tag)
				field := gjson.Get(string(payload), conf.Topic[i].Parse[j].Field)
				log.Debugf("Topic: %s    Tag: %s     Field: %v ", topic, tag.String(), field.Float())
				p := influx.NewPointWithMeasurement(conf.Influx.Bucket).
					AddTag("unit", string(tag.Str)).
					AddField("Value", field).
					SetTime(time.Now())
				writeAPI.WritePoint(p)
			}
			writeAPI.Flush()
		}
	}
}
func readConfig(filen string) Config {
	yamlFile, err := ioutil.ReadFile(filen)
	if err != nil {
		log.Fatal(err)
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}

func sub(client mqtt.Client) {
	for i := 0; i < len(conf.Topic); i++ {
		topic := conf.Topic[i].Topic
		topic_list = append(topic_list, topic)
		token := client.Subscribe(topic, 1, nil)
		token.Wait()
		log.Infof("Subscribed to topic: %s \n", topic)
	}

}
func main() {
	initLogger()
	conf = readConfig("/etc/mqttConfig.yaml")
	var broker = conf.Broker.IP
	log.Infof("Broker: %s", broker)
	var port = conf.Broker.Port
	log.Infof("Port: %v", port)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID("go_mqtt_client")
	//opts.SetUsername("emqx")
	//opts.SetPassword("public")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	sub(client)

	for {
		time.Sleep(time.Second)
	}

}
