package simple

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_Consumer_GetTimeBasedOffsetsPerPartition(t *testing.T) {
	topic := "topic"
	since := mustCreateTime(t, "2020-01-01T12:00:00Z")
	dummyClient := client(topic).
		partitionIDs([]int32{0}, nil).
		partition(0, partitionConfig{
			oldest: offset{offset: 0},
			newest: offset{offset: 10},
			messages: map[int64]messageAtOffset{
				4: {
					msg: &sarama.ConsumerMessage{Timestamp: since},
				},
			},
		}).build()
	// The message is invalid as the time extractor we use require a timestamp header
	invalidMessage := &sarama.ConsumerMessage{}

	testCases := map[string]struct {
		globalTimeout   time.Duration
		client          *clientMock
		expectedOffsets map[int32]int64
		expectedErr     error
	}{
		"success - multiple partitions": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0, 1, 2}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since},
						},
					},
				}).
				partition(1, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						0: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-3 * time.Hour)},
						},
						1: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-2 * time.Hour)},
						},
						2: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-1 * time.Hour)},
						},
						3: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(1 * time.Hour)},
						},
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(2 * time.Hour)},
						},
					},
				}).
				partition(2, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-4 * time.Hour)},
						},
						5: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-3 * time.Hour)},
						},
						6: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-2 * time.Hour)},
						},
						7: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-1 * time.Hour)},
						},
						8: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(1 * time.Hour)},
						},
						9: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(2 * time.Hour)},
						},
					},
				}).build(),
			expectedOffsets: map[int32]int64{
				0: 4,
				1: 2,
				2: 7,
			},
		},
		"success - all inside": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						0: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(5 * time.Hour)},
						},
						1: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(6 * time.Hour)},
						},
						2: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(7 * time.Hour)},
						},
						3: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(8 * time.Hour)},
						},
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(9 * time.Hour)},
						},
					},
				}).build(),
			expectedOffsets: map[int32]int64{
				0: 0,
			},
		},
		"success - all outside": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-10 * time.Hour)},
						},
						5: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-9 * time.Hour)},
						},
						6: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-8 * time.Hour)},
						},
						7: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-7 * time.Hour)},
						},
						8: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-6 * time.Hour)},
						},
						9: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-5 * time.Hour)},
						},
					},
				}).build(),
			expectedOffsets: map[int32]int64{
				0: 9,
			},
		},
		"error - get partitions": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs(nil, errors.New("foo")).build(),
			expectedErr: errors.New("foo"),
		},
		"error - timeout": {
			globalTimeout: time.Nanosecond,
			client:        dummyClient,
			expectedErr:   errors.New("context cancelled before collecting partition responses: context deadline exceeded"),
		},
		"error - get oldest offset": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{err: errors.New("foo")},
				}).build(),
			expectedErr: errors.New("foo"),
		},
		"error - get newest offset": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{err: errors.New("foo")},
				}).build(),
			expectedErr: errors.New("foo"),
		},
		"error - invalid message": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: invalidMessage,
						},
					},
				}).build(),
			expectedErr: errors.New("error while executing comparator: empty time"),
		},
		"error - get message offset": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since},
							err: errors.New("foo"),
						},
					},
				}).build(),
			expectedErr: errors.New("error while retrieving message offset 4 on partition 0: foo"),
		},
		"error - out of range offset": {
			globalTimeout: time.Second,
			client: client(topic).
				partitionIDs([]int32{0}, nil).
				partition(0, partitionConfig{
					oldest: offset{offset: 0},
					newest: offset{offset: 10},
					messages: map[int64]messageAtOffset{
						4: {
							msg: &sarama.ConsumerMessage{Timestamp: since},
							err: &outOfRangeOffsetError{
								message: "foo",
							},
						},
						5: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-3 * time.Hour)},
						},
						6: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-2 * time.Hour)},
						},
						7: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(-1 * time.Hour)},
						},
						8: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(1 * time.Hour)},
						},
						9: {
							msg: &sarama.ConsumerMessage{Timestamp: since.Add(2 * time.Hour)},
						},
					},
				}).build(),
			expectedOffsets: map[int32]int64{
				0: 7,
			},
		},
	}
	for name, tt := range testCases {
		t.Run(name, func(t *testing.T) {
			consumer, err := newDurationClient(tt.client)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), tt.globalTimeout)
			defer cancel()

			offsets, err := consumer.getTimeBasedOffsetsPerPartition(ctx, topic, since, kafkaHeaderTimeExtractor)
			if tt.expectedErr != nil {
				assert.EqualError(t, err, tt.expectedErr.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedOffsets, offsets)
			}
		})
	}
}

type clientBuilder struct {
	mock  *clientMock
	topic string
}

func client(topic string) clientBuilder {
	return clientBuilder{
		mock:  new(clientMock),
		topic: topic,
	}
}

func (c clientBuilder) partitionIDs(partitionIDs []int32, err error) clientBuilder {
	c.mock.On("getPartitionIDs", c.topic).Return(partitionIDs, err)
	return c
}

type partitionConfig struct {
	oldest   offset
	newest   offset
	messages map[int64]messageAtOffset
}

type offset struct {
	offset int64
	err    error
}

type messageAtOffset struct {
	msg *sarama.ConsumerMessage
	err error
}

func (c clientBuilder) partition(partitionID int32, partitionConfig partitionConfig) clientBuilder {
	c.mock.On("getOldestOffset", c.topic, partitionID).Return(partitionConfig.oldest.offset, partitionConfig.oldest.err)
	c.mock.On("getNewestOffset", c.topic, partitionID).Return(partitionConfig.newest.offset, partitionConfig.newest.err)

	for offset, message := range partitionConfig.messages {
		c.mock.On("getMessageAtOffset", mock.Anything, c.topic, partitionID, offset).Return(message.msg, message.err)
	}

	return c
}

func (c clientBuilder) build() *clientMock {
	return c.mock
}

type clientMock struct {
	mock.Mock
}

func (c *clientMock) getPartitionIDs(topic string) ([]int32, error) {
	args := c.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *clientMock) getOldestOffset(topic string, partitionID int32) (int64, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).(int64), args.Error(1)
}

func (c *clientMock) getNewestOffset(topic string, partitionID int32) (int64, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).(int64), args.Error(1)
}

func (c *clientMock) getMessageAtOffset(ctx context.Context, topic string, partitionID int32, offset int64) (*sarama.ConsumerMessage, error) {
	args := c.Called(ctx, topic, partitionID, offset)
	return args.Get(0).(*sarama.ConsumerMessage), args.Error(1)
}

func mustCreateTime(t *testing.T, timestamp string) time.Time {
	ts, err := time.Parse(time.RFC3339, timestamp)
	require.NoError(t, err)
	return ts
}

func kafkaHeaderTimeExtractor(msg *sarama.ConsumerMessage) (time.Time, error) {
	if msg.Timestamp.IsZero() {
		return time.Time{}, errors.New("empty time")
	}
	return msg.Timestamp, nil
}
