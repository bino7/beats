package mysql

import (
	"github.com/elastic/beats/v7/libbeat/outputs/mysql/models"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"sync/atomic"
)

type batch struct {
	publisher.Batch
	client *client
	count  int32
	total  int
	failed []publisher.Event
	err    error
}

func (b *batch) done() {
	b.dec()
}

func (b *batch) fail(r *record, err error) {
	switch err {
	case models.ModelNotFoundErr:
		log.Error("Mysql : dropping invalid message,(model=%v) not found", r.model)
	case emptyDataErr:
		log.Error("Mysql : dropping invalid message,(model=%v) data is empty", r.model)
	default:
		b.failed = append(b.failed, r.event)
		b.err = err
	}
	b.dec()
}

func (b *batch) dec() {
	i := atomic.AddInt32(&b.count, -1)
	if i > 0 {
		return
	}

	log.Debug("finished kafka batch")
	stats := b.client.observer

	err := b.err
	if err != nil {
		failed := len(b.failed)
		success := b.total - failed
		b.RetryEvents(b.failed)

		stats.Failed(failed)
		if success > 0 {
			stats.Acked(success)
		}

		log.Debugf("Kafka publish failed with: %v", err)
	} else {
		b.ACK()
		stats.Acked(b.total)
	}
}
