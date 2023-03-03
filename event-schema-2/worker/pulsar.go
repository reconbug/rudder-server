package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/rudderlabs/rudder-server/config"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const hashSize = 65536

// NewPulsarClient creates a pulsar client by reading the client configuration from the config object
func NewPulsarClient(conf *config.Config, log logger.Logger) (pulsar.Client, error) {
	url := conf.GetString("PULSAR_CLIENT_URL", "pulsar://localhost:6650")
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		OperationTimeout:  conf.GetDuration("PULSAR_CLIENT_OPERATION_TIMEOUT", 30, time.Second),
		ConnectionTimeout: conf.GetDuration("PULSAR_CLIENT_CONNECTION_TIMEOUT", 30, time.Second),
		Logger:            &pulsarLogAdapter{Logger: log},
	})
}

// NewPulsarConsumer creates new consumer from the provided client that consumes messages from the event-schema topic.
// The consumer is configured to use a KeyShared subscription mode with a sticky hash range that is specific to the node.
// The hash range is calculated based on the node index and the total number of nodes.
func NewPulsarConsumer(client pulsar.Client, conf *config.Config) (pulsar.Consumer, error) {
	nodeIdx := conf.GetInt("PULSAR_CONSUMER_NODE_IDX", 0)
	totalNodes := conf.GetInt("PULSAR_CONSUMER_TOTAL_NODES", 1)
	hashRanges, err := hashRanges(nodeIdx, totalNodes)
	if err != nil {
		return nil, err
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Name:             fmt.Sprintf("worker-%d-%d", nodeIdx, totalNodes),
		Topic:            "event-schema",
		SubscriptionName: "worker",
		Type:             pulsar.KeyShared,
		KeySharedPolicy: &pulsar.KeySharedPolicy{
			Mode:                    pulsar.KeySharedPolicyModeSticky,
			HashRanges:              hashRanges,
			AllowOutOfOrderDelivery: false,
		},
		ReceiverQueueSize:           conf.GetInt("PULSAR_CONSUMER_RECEIVER_QUEUE_SIZE", 1000),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	return consumer, err
}

// ConsumerLoop reads messages from the pulsar consumer and calls the handler's Handle method for each message
// until the context is cancelled or an error occurs.
func ConsumerLoop(ctx context.Context, consumer pulsar.Consumer, handler Handler, stat stats.Stats, log logger.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := consumer.Receive(ctx)
		start := time.Now()
		if err != nil {
			return err
		}

		correlationID := msg.ID().Serialize()
		message, err := eventschema2.UnmarshalEventSchemaMessage(msg.Payload())
		if err != nil {
			log.Errorw("Failed to unmarshal message", "consumer", consumer.Name(), "payload", string(msg.Payload()), "error", err)
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		message.CorrelationID = correlationID
		log.Debugw("Received event schema message", "consumer", consumer.Name(), "writeKey", message.Key.WriteKey)
		if err := handler.Handle(message); err != nil {
			if errors.Is(err, ErrMessageExpired) {
				_ = consumer.AckID(msg.ID())
			} else {
				log.Errorw("Failed to handle message", "consumer", consumer.Name(), "message", message.String(), "error", err)
				return fmt.Errorf("failed to handle message: %w", err)
			}
		}
		stat.NewTaggedStat("event_schema_consumer_receive", stats.TimerType, stats.Tags{"consumer": consumer.Name()}).Since(start)
	}
}

// NewPulsarConsumer creates new producer from the provided client that produces messages to the event-schema topic.
func NewPulsarProducer(client pulsar.Client, conf *config.Config) (pulsar.Producer, error) {
	return client.CreateProducer(pulsar.ProducerOptions{
		Topic: "event-schema",
	})
}

func hashRanges(nodeIdx, totalNodes int) ([]int, error) {
	if totalNodes < 1 || totalNodes > hashSize {
		return nil, fmt.Errorf("totalNodes must be between 1 and %d", hashSize)
	}
	if nodeIdx < 0 || nodeIdx >= totalNodes {
		return nil, fmt.Errorf("nodeIdx must be between 0 and %d (totalNodes-1)", totalNodes-1)
	}
	step := hashSize / totalNodes

	start := nodeIdx * step
	end := start + step - 1
	if nodeIdx == totalNodes-1 {
		end = hashSize - 1
	}
	return []int{start, end}, nil
}

// pulsarAcker implements the Acker interface for pulsar (deserializing the correlationID to a pulsar.MessageID)
type pulsarAcker struct {
	delegate pulsar.Consumer
}

func (a *pulsarAcker) AckID(correlationID []byte) error {
	msgID, err := pulsar.DeserializeMessageID(correlationID)
	if err != nil {
		return err
	}
	return a.delegate.AckID(msgID)
}

type pulsarLogAdapter struct {
	logger.Logger
}

func (pl *pulsarLogAdapter) SubLogger(fields pulsarlog.Fields) pulsarlog.Logger {
	farr := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		farr = append(farr, k, v)
	}
	return &pulsarLogAdapter{pl.Logger.With(farr...)}
}

func (pl *pulsarLogAdapter) WithFields(fields pulsarlog.Fields) pulsarlog.Entry {
	return pl.SubLogger(fields)
}

func (pl *pulsarLogAdapter) WithField(name string, value interface{}) pulsarlog.Entry {
	return &pulsarLogAdapter{pl.Logger.With(name, value)}
}

func (pl *pulsarLogAdapter) WithError(err error) pulsarlog.Entry {
	return pl.WithField("error", err)
}
