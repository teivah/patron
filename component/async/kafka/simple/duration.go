package simple

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beatlabs/patron/component/async/kafka"
	"github.com/beatlabs/patron/log"
)

type durationClient struct {
	client durationKafkaClientAPI
}

func newDurationClient(client durationKafkaClientAPI) (durationClient, error) {
	if client == nil {
		return durationClient{}, errors.New("empty client api")
	}
	return durationClient{client: client}, nil
}

func (d durationClient) getTimeBasedOffsetsPerPartition(ctx context.Context, topic string, since time.Time, timeExtractor kafka.TimeExtractor) (map[int32]offsets, error) {
	partitionIDs, err := d.client.getPartitionIDs(topic)
	if err != nil {
		return nil, err
	}

	responseCh := make(chan partitionOffsetResponse, 1)
	d.triggerWorkers(ctx, topic, since, timeExtractor, partitionIDs, responseCh)

	numberOfPartitions := len(partitionIDs)
	offsets := make(map[int32]offsets, numberOfPartitions)
	numberOfResponses := 0
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled before collecting partition responses: %w", ctx.Err())
		case response := <-responseCh:
			if response.err != nil {
				return nil, response.err
			}

			offsets[response.partitionID] = response.offsets
			numberOfResponses++
			if numberOfResponses == numberOfPartitions {
				return offsets, nil
			}
		}
	}
}

func (d durationClient) triggerWorkers(ctx context.Context, topic string, since time.Time, timeExtractor kafka.TimeExtractor, partitionIDs []int32, responseCh chan<- partitionOffsetResponse) {
	for _, partitionID := range partitionIDs {
		partitionID := partitionID
		go func() {
			offset, err := d.getTimeBasedOffset(ctx, topic, since, partitionID, timeExtractor)
			responseCh <- partitionOffsetResponse{
				partitionID: partitionID,
				offsets:     offset,
				err:         err,
			}
		}()
	}
}

type partitionOffsetResponse struct {
	partitionID int32
	offsets     offsets
	err         error
}

type offsets struct {
	timeOffset   int64
	latestOffset int64
}

func (d durationClient) getTimeBasedOffset(ctx context.Context, topic string, since time.Time, partitionID int32, timeExtractor kafka.TimeExtractor) (offsets, error) {
	left, err := d.client.getOldestOffset(topic, partitionID)
	if err != nil {
		return offsets{}, err
	}

	latestOffset, err := d.client.getNewestOffset(topic, partitionID)
	if err != nil {
		return offsets{}, err
	}
	latestOffset--
	right := latestOffset

	// Binary search implementation
	for left <= right {
		mid := left + (right-left)/2

		msg, err := d.client.getMessageAtOffset(ctx, topic, partitionID, mid)
		if err != nil {
			// Under extraordinary circumstances (e.g. the retention policy being applied just before retrieving the message at a particular offset),
			// the offset might not be accessible anymore.
			// In this case, we simply log a warning and restrict the interval to the right.
			if errors.Is(err, &outOfRangeOffsetError{}) {
				log.Warnf("offset %d on partition %d is out of range: %v", mid, partitionID, err)
				left = mid + 1
				continue
			}
			return offsets{}, fmt.Errorf("error while retrieving message offset %d on partition %d: %w", mid, partitionID, err)
		}

		t, err := timeExtractor(msg)
		if err != nil {
			return offsets{}, fmt.Errorf("error while executing comparator: %w", err)
		}

		if t.Equal(since) {
			return offsets{
				timeOffset:   mid,
				latestOffset: latestOffset,
			}, nil
		}
		if t.Before(since) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if left == 0 {
		return offsets{
			timeOffset:   0,
			latestOffset: latestOffset,
		}, nil
	}
	return offsets{
		timeOffset:   left - 1,
		latestOffset: latestOffset,
	}, nil
}
