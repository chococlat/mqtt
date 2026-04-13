package mqttclient

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
	stdlog "log"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)


type OutputMessage struct {
	Topic   string
	Payload []byte
	Qos     byte
}

type MQTTClient struct {
	client    mqtt.Client
	inputCh   chan mqtt.Message
	outputCh  chan OutputMessage
	stopOnce  sync.Once // (1) guards Stop() against double-close panics

	broker   string
	port     int
	clientID string
	username string
	password string
}

// NewMQTTClient creates a robust MQTT client
func NewMQTTClient(broker string, port int, topics map[string]byte, clientID, username, password string, debug bool) (*MQTTClient, error) {

	if debug {
		mqtt.WARN = stdlog.New(os.Stdout, "", 0)
		mqtt.ERROR = stdlog.New(os.Stdout, "", 0)
		mqtt.DEBUG = stdlog.New(os.Stdout, "", 0)
	}

	if broker == "" || clientID == "" || len(topics) == 0 {
		return nil, fmt.Errorf("invalid args: broker, clientID, topics required")
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)
	opts.SetCleanSession(false)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)

	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}

	c := &MQTTClient{
		inputCh:   make(chan mqtt.Message, 256),
		outputCh:  make(chan OutputMessage, 256),
		broker:    broker,
		port:      port,
		clientID:  clientID,
		username:  username,
		password:  password,
	}

	opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		log.Info().Str("topic", msg.Topic()).Msg("message received in default handler")
		select {
		case c.inputCh <- msg:
			log.Info().Str("topic", msg.Topic()).Msg("message sent to inputCh")
		default:
			log.Warn().Str("topic", msg.Topic()).Msg("inputCh full, dropping message")
		}
	})

	opts.OnConnect = func(cli mqtt.Client) {
		log.Info().Msg("Connected; subscribing to topics...")
		for t, q := range topics {
			if token := cli.Subscribe(t, q, nil); token.Wait() && token.Error() != nil {
				log.Error().Err(token.Error()).Msgf("subscribe failed for %s", t)
			} else {
				log.Info().Msgf("subscribed %s", t)
			}
		}
	}

	opts.SetConnectionLostHandler(func(cli mqtt.Client, err error) {
		log.Error().Err(err).Msg("connection lost")
	})

	c.client = mqtt.NewClient(opts)
	return c, nil
}

// Connect with exponential backoff until context canceled
func (c *MQTTClient) Connect(ctx context.Context) error {
	// (2) bail immediately if context is already cancelled before attempting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	backoff := 1 * time.Second
	log.Info().Msgf("connecting to %s:%d...", c.broker, c.port)
	for {
		if token := c.client.Connect(); token.Wait() && token.Error() == nil {
			log.Info().Msg("connected to MQTT broker")
			return nil
		} else {
			log.Error().Err(token.Error()).Msgf("connect failed, retrying in %s", backoff)
		}

		select {
		case <-time.After(backoff):
			if backoff < 30*time.Second {
				backoff *= 2
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Run starts a watchdog that reconnects if the client drops
func (c *MQTTClient) Run(ctx context.Context) error {
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if !c.client.IsConnected() {
					log.Warn().Msg("client disconnected, reconnecting...")
					_ = c.Connect(ctx)
				}
			}
		}
	}()
	return nil
}

func (c *MQTTClient) GetInputChannel() chan mqtt.Message {
	return c.inputCh
}

func (c *MQTTClient) GetOutputChannel() chan OutputMessage {
	return c.outputCh
}

// Publish sends a message using the same client
func (c *MQTTClient) Publish(topic string, payload []byte, qos byte, retained bool) error {
	if token := c.client.Publish(topic, qos, retained, payload); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// Stop disconnects the client and closes channels exactly once
func (c *MQTTClient) Stop() {
	// (1) sync.Once prevents a second call from closing already-closed channels and panicking
	c.stopOnce.Do(func() {
		c.client.Disconnect(250)
		close(c.inputCh)
		close(c.outputCh)
	})
}

func (c *MQTTClient) StartPublishWorker(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("shutting down publish worker")
				return
			case msg := <-c.outputCh: // (3) removed spammy "waiting..." log that fired on every iteration
				if err := c.Publish(msg.Topic, msg.Payload, msg.Qos, false); err != nil {
					log.Error().Err(err).Msg("failed to publish message")
				} else {
					log.Info().Str("topic", msg.Topic).Msg("message published")
				}
			}
		}
	}()
}
