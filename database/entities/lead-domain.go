package entities

import "time"

type LeadDomain struct {
	ID        uint      `gorm:"column:id;type:bigSerial;primary_key;not null"`
	Name      string    `gorm:"column:name;type:varchar(50);unique;index;"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp;default:current_timestamp;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp;default:current_timestamp;not null"`
}
