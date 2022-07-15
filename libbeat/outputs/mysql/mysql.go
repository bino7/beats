package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/mysql/models"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"gorm.io/gorm"
	"time"
)

var (
	emptyDataErr = errors.New("record has no data")
)

func InitMysql() {
	outputs.RegisterType("mysql", makeMysql)
}

const (
	defaultWaitRetry = 1 * time.Second

	// NOTE: maxWaitRetry has no effect on mode, as logstash client currently does
	// not return ErrTempBulkFailure
	defaultMaxWaitRetry = 60 * time.Second

	debugSelector = "mysql"
)

const (
	logSelector = "mysql"
)

var log = logp.NewLogger(debugSelector)

type record struct {
	model string
	data  []byte
	event publisher.Event
}

func (r *record) insert(db *gorm.DB) error {
	model, err := models.Get(r.model)
	if err != nil {
		return err
	}
	db.AutoMigrate(model)
	err = json.Unmarshal(r.data, &model)
	if err != nil {
		return fmt.Errorf("json unmarshal error:%e", err)
	}
	tx := db.Save(model)
	return tx.Error
}

func makeMysql(
	im outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *config.C) (outputs.Group, error) {
	log := logp.NewLogger(logSelector)
	log.Debug("initialize kafka output")

	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	retry := 0
	if config.Metadata.Retry.Max < 0 {
		retry = -1
	}
	mysqlConfig, _ := newMysqlConfig(config)
	client, err := newMysqlClient(observer, mysqlConfig)
	if err != nil {
		return outputs.Fail(err)
	}
	return outputs.Success(config.BulkMaxSize, retry, client)
}
