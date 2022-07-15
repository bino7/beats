package mysql

import (
	"context"
	"errors"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/mysql/models"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/mapstr"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
)

var errNoModelName = errors.New("no model name could be selected")
var errNoData = errors.New("no data was found")

type client struct {
	db       *gorm.DB
	mux      sync.Mutex
	observer outputs.Observer
	key      *fmtstr.EventFormatString
	topic    outil.Selector
	cfg      *mysql.Config
}

func (c *client) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	log.Debugf("closed mysql client")
	return nil
}

func (c *client) Publish(ctx context.Context, publisherBatch publisher.Batch) error {
	events := publisherBatch.Events()
	batch := &batch{
		Batch:  publisherBatch,
		client: c,
		count:  int32(len(events)),
		total:  len(events),
		failed: nil,
	}
	c.observer.NewBatch(len(events))
	for _, d := range events {
		r, err := c.getRecord(&d)
		if err != nil {
			switch err {
			case models.ModelNotFoundErr:
				log.Error("Mysql : dropping invalid message,(model=%v) not found", r.model)
			case emptyDataErr:
				log.Error("Mysql : dropping invalid message,(model=%v) data is empty", r.model)
			default:
				log.Error("Dropping event: %v", err)
			}
			batch.done()
			c.observer.Dropped(1)
			return err
		}
		err = r.insert(c.db)
		if err != nil {
			batch.fail(r, err)
			c.observer.Failed(1)
		}
		batch.done()
	}
	return nil
}

func (c *client) String() string {
	return "mysql(" + c.cfg.DSN + ")"
}

func (c *client) Connect() error {
	db, err := gorm.Open(mysql.Open(c.cfg.DSN), &gorm.Config{})
	if err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *client) getRecord(d *publisher.Event) (*record, error) {
	r := &record{
		event: *d,
	}
	event := &d.Content
	value, err := event.GetValue("headers")
	if err != nil || value == nil {
		err = errNoModelName
		return nil, err
	}
	value, err = value.(mapstr.M).GetValue("Model")
	if err != nil {
		err = errNoModelName
		return nil, err
	}
	model := value.([]string)[0]
	if model == "" {
		err = errNoModelName
		return nil, err
	}
	r.model = model

	value, err = event.GetValue("data")
	if err != nil {
		err = errNoData
		return r, err
	}
	value, err = value.(mapstr.M).GetValue("data")
	if err != nil {
		err = errNoData
		return r, err
	}
	r.data = []byte(value.(string))

	return r, nil
}

func newMysqlClient(
	observer outputs.Observer,
	cfg *mysql.Config) (*client, error) {
	return &client{
		observer: observer,
		cfg:      cfg,
	}, nil
}
