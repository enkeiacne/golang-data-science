package entities

import "time"

type LeadFileHistory struct {
	ID        uint      `gorm:"column:id;type:bigSerial;primary_key;not null"`
	FileName  string    `gorm:"column:file_name;type:varchar(255);index;not null;"`
	Status    string    `gorm:"column:status;type:varchar(50);index;not null"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp;default:current_timestamp;index;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp;default:current_timestamp;not null"`
}
