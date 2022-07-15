package mysql

import (
	"fmt"
	"github.com/elastic/elastic-agent-libs/config"
	"gorm.io/driver/mysql"
	"time"
)

type mysqlConfig struct {
	Host               string        `config:"host" validate:"required"`
	Port               int           `config:"port"`
	Username           string        `config:"username" validate:"required"`
	Password           string        `config:"password" validate:"required"`
	DatabaseName       string        `config:"database" validate:"required"`
	Metadata           metaConfig    `config:"metadata"`
	BulkMaxSize        int           `config:"bulk_max_size"`
	BulkFlushFrequency time.Duration `config:"bulk_flush_frequency"`
	MaxRetries         int           `config:"max_retries"         validate:"min=-1,nonzero"`
}

type metaConfig struct {
	Retry       metaRetryConfig `config:"retry"`
	RefreshFreq time.Duration   `config:"refresh_frequency" validate:"min=0"`
	Full        bool            `config:"full"`
}

type metaRetryConfig struct {
	Max     int           `config:"max"     validate:"min=0"`
	Backoff time.Duration `config:"backoff" validate:"min=0"`
}

func defaultConfig() mysqlConfig {
	return mysqlConfig{}
}

func readConfig(cfg *config.C) (*mysqlConfig, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *mysqlConfig) Validate() error {
	return nil
}

func newMysqlConfig(config *mysqlConfig) (*mysql.Config, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)
	return &mysql.Config{
		DSN: dsn,
	}, nil
}
