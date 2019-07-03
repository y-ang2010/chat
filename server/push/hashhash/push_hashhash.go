package hashhash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tinode/chat/server/push"
)

var handler Handler

// Size of the input channel buffer.
const (
	defaultBuffer    = 32
	TOPIC_KEY_MSG    = "msg"
	TOPIC_KEY_BUBBLE = "bubble"
)

type KafkaConfig struct {
	Brokers   []string          `json:"brokers"`
	Topics    map[string]string `json:"topics"`
	Partition string            `json:"partition"`
}
type KafkaWriters struct {
	kwMsg    *kafka.Writer
	kwBubble *kafka.Writer
}

// Handler represents the push handler; implements push.PushHandler interface.
type Handler struct {
	input  chan *push.Receipt
	stop   chan bool
	client *KafkaWriters
}

type configType struct {
	Enabled bool         `json:"enabled"`
	Kafka   *KafkaConfig `json:"kafka"`
	Buffer  int          `json:"buffer"`
}

type MsgMedias struct {
	Bucket      string `json:"bucket,omitempty"`
	Key         string `json:"key,omitempty"`
	Thumbnail   string `json:"thumbnail,omitempty"`
	ImageHeight string `json:"imageHeight,omitempty"`
	ImageWidth  string `json:"imageWidth,omitempty"`
}
type MsgStruct struct {
	Type         string     `json:"type,omitempty"`         // msg type [0-Text, 1-AudioMode,2-ImageMode,3-VideoMode,4-]
	From         string     `json:"from,omitempty"`         // 消息来源：普通聊天[默认为空]、泡泡聊天[BOBL]、其他
	Text         string     `json:"text,omitempty"`         // text
	AudioMode    *MsgMedias `json:"audioModel,omitempty"`   // json string for MsgMedias{}
	ImageMode    *MsgMedias `json:"imageModel,omitempty"`   // json string for MsgMedias{}
	VideoMode    *MsgMedias `json:"videoModel,omitempty"`   // json string for MsgMedias{}
	CardMode     string     `json:"cardModel,omitempty"`    // json string
	CheckeStatus string     `json:"CheckeStatus,omitempty"` //[0-未审核，1-审核通过，2-审核不通过]
}
type BosFilePath struct {
	Bucket   string `json:"bucket" `
	Region   string `json:"Region" `
	FilePath string `json:"filePath" `
}

func newKafkaWriter(c *kafka.WriterConfig) (*kafka.Writer, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for k, v := range c.Brokers {
		_, err := kafka.DialContext(ctx, "tcp", v)
		if err != nil {
			panic(fmt.Errorf("the broker dial err %v and the index is %d", err, k))
			//return nil, fmt.Errorf("the broker dial err %v and the index is %d", err, k)
		}
	}
	writer := kafka.NewWriter(*c)
	return writer, nil
}
func decMsg(content interface{}) (*MsgStruct, error) {
	s, ok := content.(string)
	if !ok {
		return nil, fmt.Errorf("content is not string")
	}
	m := &MsgStruct{}
	err := json.Unmarshal([]byte(s), m)
	if err != nil {
		return nil, fmt.Errorf("[%v] unmarshal error:%v", reflect.TypeOf(m).Elem().Name(), err)
	}
	return m, nil
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
	cli := &KafkaWriters{}
	// init kafka writer for normal msg
	topicMsg, ok := config.Kafka.Topics[TOPIC_KEY_MSG]
	if !ok {
		return fmt.Errorf("get [%v] topic nil", TOPIC_KEY_MSG)
	}
	cli.kwMsg, err = newKafkaWriter(&kafka.WriterConfig{
		Brokers: config.Kafka.Brokers,
		Topic:   topicMsg,
	})
	if err != nil {
		return err
	}

	// init kafka writer for Bubble msg
	topicBubble, ok := config.Kafka.Topics[TOPIC_KEY_BUBBLE]
	if !ok {
		return fmt.Errorf("get [%v] topic nil", TOPIC_KEY_BUBBLE)
	}
	cli.kwBubble, err = newKafkaWriter(&kafka.WriterConfig{
		Brokers: config.Kafka.Brokers,
		Topic:   topicBubble,
	})
	if err != nil {
		return err
	}

	if config.Buffer <= 0 {
		config.Buffer = defaultBuffer
	}
	handler.client = cli
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
	data["from"] = pl.From

	if m, err := decMsg(pl.Content); err == nil && m != nil && m.From == "BUBL" && m.AudioMode != nil {
		// ms
		data["ts"] = fmt.Sprintf("%v", pl.Timestamp.UnixNano()/1e6)
		a := m.AudioMode
		filePath := &BosFilePath{
			Bucket:   a.Bucket,
			Region:   "bj",
			FilePath: a.Key,
		}
		b, _ := json.Marshal(filePath)
		data["bosfile"] = string(b)
	} else {
		data["ts"] = pl.Timestamp.Format(time.RFC3339Nano)
		data["seq"] = strconv.Itoa(pl.SeqId)
		data["content"] = fmt.Sprintf("%v", pl.Content)
		if len(data["content"]) > 128 {
			data["content"] = data["content"][:128]
		}
	}

	return data, nil
}

func sendNotifications(rcpt *push.Receipt, config *configType) {
	ctx := context.Background()

	data, _ := payloadToData(&rcpt.Payload)
	if data == nil || (data["content"] == "" && data["bosfile"] == "") {
		log.Println("fcm push: could not parse payload or empty payload")
		return
	}
	var w kafka.Writer
	if data["content"] != "" {
		w = *handler.client.kwMsg
	} else {
		w = *handler.client.kwBubble
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
		err = w.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("kafka [%v] write msg error:%v\n", w.Stats().Topic, err)
		}
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
