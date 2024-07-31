package entities

import "time"

type Lead struct {
	ID        uint      `gorm:"column:id;type:bigSerial;primary_key;not null"`
	Phone     string    `gorm:"column:phone;type:varchar(50);"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp;default:current_timestamp;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp;default:current_timestamp;not null"`
}
