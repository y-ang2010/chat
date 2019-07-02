package hashhash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tinode/chat/server/push"
)

var handler Handler

// Size of the input channel buffer.
const defaultBuffer = 32

type KafkaConfig struct {
	Brokers   []string `json:"brokers"`
	Topic     string   `json:"topic"`
	Partition string   `json:"partition"`
}

// Handler represents the push handler; implements push.PushHandler interface.
type Handler struct {
	input  chan *push.Receipt
	stop   chan bool
	client *kafka.Writer
}

type configType struct {
	Enabled bool         `json:"enabled"`
	Kafka   *KafkaConfig `json:"kafka"`
	Buffer  int          `json:"buffer"`
}

func newKafkaWriter(c *kafka.WriterConfig) (*kafka.Writer, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for k, v := range c.Brokers {
		_, err := kafka.DialContext(ctx, "tcp", v)
		if err != nil {
			panic(fmt.Errorf("the broker dial err %v and the index is %d", err, k))
			return nil, fmt.Errorf("the broker dial err %v and the index is %d", err, k)

		}
	}
	writer := kafka.NewWriter(*c)
	return writer, nil
}

// Init initializes the push handler
func (Handler) Init(jsonconf string) error {

	var config configType
	err := json.Unmarshal([]byte(jsonconf), &config)
	if err != nil {
		return errors.New("failed to parse config: " + err.Error())
	}

	if !config.Enabled {
		return nil
	}

	handler.client, err = newKafkaWriter(&kafka.WriterConfig{
		Brokers: config.Kafka.Brokers,
		Topic:   config.Kafka.Topic,
	})
	if err != nil {
		return err
	}

	if config.Buffer <= 0 {
		config.Buffer = defaultBuffer
	}

	handler.input = make(chan *push.Receipt, config.Buffer)
	handler.stop = make(chan bool, 1)

	go func() {
		for {
			select {
			case rcpt := <-handler.input:
				go sendNotifications(rcpt, &config)
			case <-handler.stop:
				return
			}
		}
	}()

	return nil
}

func payloadToData(pl *push.Payload) (map[string]string, error) {
	if pl == nil {
		return nil, nil
	}

	data := make(map[string]string)
	data["topic"] = pl.Topic
	// Must use "xfrom" because "from" is a reserved word.
	// Google did not bother to document it anywhere.
	data["from"] = pl.From
	data["ts"] = pl.Timestamp.Format(time.RFC3339Nano)
	data["seq"] = strconv.Itoa(pl.SeqId)
	//data["mime"] = pl.ContentType
	data["content"] = fmt.Sprintf("%v", pl.Content)

	if len(data["content"]) > 128 {
		data["content"] = data["content"][:128]
	}

	return data, nil
}

func sendNotifications(rcpt *push.Receipt, config *configType) {
	ctx := context.Background()

	data, _ := payloadToData(&rcpt.Payload)
	if data == nil || data["content"] == "" {
		log.Println("fcm push: could not parse payload or empty payload")
		return
	}
	bData, err := json.Marshal(data)
	if err != nil {
		log.Println("json.Marshal data error:", err)
		return
	}

	for _, to := range rcpt.To {
		key := []byte(to.User.UserId())
		if len(key) == 0 {
			continue
		}
		msg := kafka.Message{
			Key:   []byte(to.User.UserId()),
			Value: bData,
		}
		fmt.Println("send msg :", string(bData))
		_ = handler.client.WriteMessages(ctx, msg)
	}
}

// IsReady checks if the push handler has been initialized.
func (Handler) IsReady() bool {
	return handler.input != nil
}

// Push return a channel that the server will use to send messages to.
// If the adapter blocks, the message will be dropped.
func (Handler) Push() chan<- *push.Receipt {
	return handler.input
}

// Stop shuts down the handler
func (Handler) Stop() {
	handler.stop <- true
}

func init() {
	push.Register("hashhash", &handler)
}
