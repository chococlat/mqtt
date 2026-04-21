package core

import (
	"context"
	"agent/internal/handler"

	"agent/internal/logging"
	"agent/internal/mqttclient"

	"agent/internal/config"

	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
)


func StartMQTTLoop() {

	logging.SetupZerolog("app.log", "INFO")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topics := map[string]byte{
		config.INPUT_TOPIC:           2,
		config.BROADCAST_INPUT_TOPIC: 2,
	}

	client, err := mqttclient.NewMQTTClient(
		config.BROKER_URL,
		config.BROKER_PORT,
		topics,
		config.MQTT_CLIENT_ID,
		config.MQTT_USERNAME,
		config.MQTT_PASSWORD,
		false, // debug
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create MQTT client")
	}

	if err := client.Connect(ctx); err != nil {
		log.Fatal().Err(err).Msg("failed to connect to broker")
	}

	client.StartPublishWorker(ctx)

	go func() {
		for msg := range client.GetInputChannel() {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error().Msgf("panic in processing: %v", r)
					}
				}()

				log.Info().Str("topic", msg.Topic()).Msgf("Calling handler for message: %s", string(msg.Payload()))

				response := handler.HandlePacket(ctx, msg.Topic(), msg.Payload())
				outputTopic := config.OUTPUT_TOPIC
				if msg.Topic() == config.BROADCAST_INPUT_TOPIC {
					outputTopic = config.BROADCAST_OUTPUT_TOPIC
				}

				client.GetOutputChannel() <- mqttclient.OutputMessage{
					Topic:   outputTopic,
					Payload: response,
					Qos:     msg.Qos(),
				}

			}()
		}
	}()

	client.Run(ctx)

	// Block until SIGINT/SIGTERM, then cancel context for clean shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigs:
		log.Info().Msg("shutting down...")
		cancel()
	case <-ctx.Done():
	}

	client.Stop()
}
