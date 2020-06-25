package simple

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beatlabs/patron/component/async/kafka"
	"github.com/beatlabs/patron/log"
)

type durationOffset struct {
	client durationClientAPI
}

func newDurationOffset(client durationClientAPI) (durationOffset, error) {
	if client == nil {
		return durationOffset{}, errors.New("empty client api")
	}
	return durationOffset{client: client}, nil
}

func (d durationOffset) getTimeBasedOffsetsPerPartition(ctx context.Context, topic string, since time.Time, timeExtractor kafka.TimeExtractor) (map[int32]int64, error) {
	partitionIDs, err := d.client.getPartitionIDs(topic)
	if err != nil {
		return nil, err
	}

	ch := make(chan partitionOffsetResponse, 1)
	for _, partitionID := range partitionIDs {
		partitionID := partitionID
		go func() {
			offset, err := d.getTimeBasedOffset(ctx, topic, since, partitionID, timeExtractor)
			ch <- partitionOffsetResponse{
				partitionID: partitionID,
				offset:      offset,
				err:         err,
			}
		}()
	}

	numberOfPartitions := len(partitionIDs)
	offsets := make(map[int32]int64, numberOfPartitions)
	numberOfResponses := 0
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled before collecting partition responses: %w", ctx.Err())
		case response := <-ch:
			if response.err != nil {
				return nil, response.err
			}

			offsets[response.partitionID] = response.offset
			numberOfResponses++
			if numberOfResponses == numberOfPartitions {
				return offsets, nil
			}
		}
	}
}

type partitionOffsetResponse struct {
	partitionID int32
	offset      int64
	err         error
}

func (d durationOffset) getTimeBasedOffset(ctx context.Context, topic string, since time.Time, partitionID int32, timeExtractor kafka.TimeExtractor) (int64, error) {
	left, err := d.client.getOldestOffset(topic, partitionID)
	if err != nil {
		return 0, err
	}

	right, err := d.client.getNewestOffset(topic, partitionID)
	if err != nil {
		return 0, err
	}

	for left <= right {
		mid := left + (right-left)/2

		msg, err := d.client.getMessageAtOffset(ctx, topic, partitionID, mid)
		if err != nil {
			// Under extraordinary circumstances (e.g. the retention policy applied just before retrieving the message at a particular offset),
			// the offset might not be accessible anymore.
			// In this case, we simply log a warning and restrict the interval on the right.
			if errors.Is(err, &outOfRangeOffsetError{}) {
				log.Warnf("offset %d on partition %d is out of range: %v", mid, partitionID, err)
				left = mid + 1
				continue
			}
			return 0, fmt.Errorf("error while retrieving message offset %d on partition %d: %w", mid, partitionID, err)
		}

		t, err := timeExtractor(msg)
		if err != nil {
			return 0, fmt.Errorf("error while executing comparator: %w", err)
		}

		if t.Equal(since) {
			return mid, nil
		}
		if t.Before(since) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if left == 0 {
		return 0, nil
	}
	return left - 1, nil
}
