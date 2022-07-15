package models

import "time"

type Log struct {
	ID    uint      `gorm:"primaryKey"`
	Level string    `gorm:"level"`
	Msg   string    `gorm:"msg"`
	Time  time.Time `gorm:"time"`
}

func (log *Log) TableName(table string) string {
	return "log"
}
