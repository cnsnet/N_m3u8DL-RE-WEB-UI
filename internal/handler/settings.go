package handler

import (
	"net/http"

	"N_m3u8DL-RE-WEB-UI/internal/service"

	"github.com/gin-gonic/gin"
)

// UpdateSettingsRequest 更新设置请求
type UpdateSettingsRequest struct {
	MaxConcurrentDownloads int `json:"max_concurrent_downloads" binding:"min=1,max=50"`
}

func GetSettings(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"max_concurrent_downloads": service.GetMaxConcurrentDownloads(),
	})
}

func UpdateSettings(c *gin.Context) {
	var req UpdateSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	if err := service.SetMaxConcurrentDownloads(req.MaxConcurrentDownloads); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"max_concurrent_downloads": req.MaxConcurrentDownloads})
}
