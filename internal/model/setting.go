package model

// Setting 系统设置（单行表，ID 固定为 1）
type Setting struct {
	ID                     uint `gorm:"primarykey" json:"id"`
	MaxConcurrentDownloads int  `gorm:"default:1" json:"max_concurrent_downloads"`
}

func (Setting) TableName() string {
	return "settings"
}
