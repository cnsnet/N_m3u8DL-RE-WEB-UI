package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"N_m3u8DL-RE-WEB-UI/internal/config"
	"N_m3u8DL-RE-WEB-UI/internal/model"
)

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	URL                string `json:"url"`
	OutputName         string `json:"output_name"`
	ThreadCount        int    `json:"thread_count"`
	RetryCount         int    `json:"retry_count"`
	Headers            string `json:"headers"`
	BaseURL            string `json:"base_url"`
	DelAfterDone       *bool  `json:"del_after_done"`
	BinaryMerge        bool   `json:"binary_merge"`
	AutoSelect         bool   `json:"auto_select"`
	SkipSegmentsCheck  bool   `json:"skip_segments_check"`
	ConcurrentDownload bool   `json:"concurrent_download"`
	Key                string `json:"key"`
	DecryptionEngine   string `json:"decryption_engine"`
	CustomArgs         string `json:"custom_args"`
	CustomProxy        string `json:"custom_proxy"`
}

func InitAdminUser(password string) {
	var count int64
	model.GetDB().Model(&model.User{}).Count(&count)
	if count == 0 {
		hashedPassword, err := model.HashPassword(password)
		if err != nil {
			log.Fatalf("密码加密失败: %v", err)
		}

		user := model.User{
			Username: "admin",
			Password: hashedPassword,
		}

		if err := model.GetDB().Create(&user).Error; err != nil {
			log.Fatalf("创建管理员用户失败: %v", err)
		}
		log.Println("管理员用户创建成功")
	}
}

func GetUserByUsername(username string) (*model.User, error) {
	var user model.User
	err := model.GetDB().Where("username = ?", username).First(&user).Error
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func ChangePassword(username, newPassword string) error {
	hashedPassword, err := model.HashPassword(newPassword)
	if err != nil {
		return err
	}
	return model.GetDB().Model(&model.User{}).Where("username = ?", username).Update("password", hashedPassword).Error
}

func CreateTask(req *CreateTaskRequest) (*model.Task, error) {
	outputName := req.OutputName
	if outputName == "" {
		outputName = generateOutputName(req.URL)
	}

	// 设置默认值
	threadCount := req.ThreadCount
	if threadCount <= 0 {
		threadCount = 16
	}
	retryCount := req.RetryCount
	if retryCount <= 0 {
		retryCount = 5
	}
	decryptionEngine := req.DecryptionEngine
	if decryptionEngine == "" {
		decryptionEngine = "MP4DECRYPT"
	}
	// 未指定时默认下载完成后删除临时文件
	delAfterDone := true
	if req.DelAfterDone != nil {
		delAfterDone = *req.DelAfterDone
	}

	task := model.Task{
		URL:                req.URL,
		Status:             model.TaskStatusPending,
		OutputName:         outputName,
		ThreadCount:        threadCount,
		RetryCount:         retryCount,
		Headers:            req.Headers,
		BaseURL:            req.BaseURL,
		DelAfterDone:       delAfterDone,
		BinaryMerge:        req.BinaryMerge,
		AutoSelect:         req.AutoSelect,
		SkipSegmentsCheck:  req.SkipSegmentsCheck,
		ConcurrentDownload: req.ConcurrentDownload,
		Key:                req.Key,
		DecryptionEngine:   decryptionEngine,
		CustomArgs:         req.CustomArgs,
		CustomProxy:        req.CustomProxy,
	}

	if err := model.GetDB().Create(&task).Error; err != nil {
		return nil, err
	}

	return &task, nil
}

func GetTaskByID(id uint) (*model.Task, error) {
	var task model.Task
	err := model.GetDB().First(&task, id).Error
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func DeleteTask(id uint) error {
	task, err := GetTaskByID(id)
	if err != nil {
		return err
	}

	// 检查进程真实存活状态，存活则直接 kill，确保删除的任务是干净的
	if task.PID > 0 && isProcessAlive(task.PID) {
		if proc, err := os.FindProcess(task.PID); err == nil {
			if err := proc.Signal(syscall.SIGKILL); err == nil {
				log.Printf("已 kill 任务 %d 的进程 (PID: %d)", id, task.PID)
			} else {
				log.Printf("kill 任务 %d 的进程失败 (PID: %d): %v", id, task.PID, err)
			}
		}
	}

	// 删除日志文件
	if task.LogFile != "" {
		os.Remove(task.LogFile)
	}

	return model.GetDB().Delete(&task).Error
}

// RetryTask 重试失败/中断的任务，复用原有下载参数，重新进入待处理队列
func RetryTask(id uint) (*model.Task, error) {
	task, err := GetTaskByID(id)
	if err != nil {
		return nil, err
	}

	if task.Status != model.TaskStatusFailed && task.Status != model.TaskStatusInterrupted {
		return nil, fmt.Errorf("只能重试失败或中断的任务")
	}

	task.Status = model.TaskStatusPending
	task.Progress = 0
	task.Speed = ""
	task.DownloadedSize = ""
	task.TotalSize = ""
	task.ErrorMsg = ""
	task.FinishedAt = nil
	task.PID = 0

	if err := model.GetDB().Save(task).Error; err != nil {
		return nil, err
	}

	return task, nil
}

// DeleteCompletedTasks 删除所有已完成的任务，返回删除数量
func DeleteCompletedTasks() (int64, error) {
	var tasks []model.Task
	if err := model.GetDB().Where("status = ?", model.TaskStatusCompleted).Find(&tasks).Error; err != nil {
		return 0, err
	}

	for _, task := range tasks {
		if task.LogFile != "" {
			os.Remove(task.LogFile)
		}
	}

	result := model.GetDB().Where("status = ?", model.TaskStatusCompleted).Delete(&model.Task{})
	return result.RowsAffected, result.Error
}

// isProcessAlive 通过发送信号0真实检测进程是否存活
func isProcessAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func GetActiveTasks() ([]model.Task, error) {
	var tasks []model.Task
	err := model.GetDB().Where("status IN ?", []string{model.TaskStatusPending, model.TaskStatusDownloading}).
		Order("created_at ASC").
		Find(&tasks).Error
	return tasks, err
}

func GetTaskLog(id uint) (string, error) {
	task, err := GetTaskByID(id)
	if err != nil {
		return "", err
	}

	if task.LogFile == "" {
		return "", nil
	}

	const headLines = 50
	const tailLines = 100
	const totalLimit = headLines + tailLines // 超过此行数则截断

	// 读开头 totalLimit+1 行，判断是否需要截断
	headAll, err := readLogHeadLines(task.LogFile, totalLimit+1)
	if err != nil {
		return "", err
	}
	allLines := strings.Split(headAll, "\n")

	// 日志 <= totalLimit 行：全量返回
	if len(allLines) <= totalLimit {
		return cleanANSI(headAll), nil
	}

	// 日志 > totalLimit 行：开头 headLines 行 + 省略提示 + 末尾 tailLines 行
	head := strings.Join(allLines[:headLines], "\n")
	tail, err := readLogTailLines(task.LogFile, tailLines)
	if err != nil {
		return "", err
	}
	content := head +
		"\n……（日志过长，中间部分已省略，仅显示开头 " + strconv.Itoa(headLines) +
		" 行和末尾 " + strconv.Itoa(tailLines) + " 行）……\n\n" +
		tail
	return cleanANSI(content), nil
}

func ListFiles(downloadDir string) ([]map[string]interface{}, error) {
	var files []map[string]interface{}

	entries, err := os.ReadDir(downloadDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, map[string]interface{}{
			"name":    entry.Name(),
			"size":    info.Size(),
			"modTime": info.ModTime().Format("2006-01-02 15:04:05"),
		})
	}

	return files, nil
}

func CleanupTaskPID(taskID uint) {
	model.GetDB().Model(&model.Task{}).Where("id = ?", taskID).Update("pid", 0)
}

func generateOutputName(url string) string {
	if idx := strings.LastIndex(url, "/"); idx != -1 {
		name := url[idx+1:]
		if len(name) > 0 && !strings.HasPrefix(name, "?") {
			return name
		}
	}
	return fmt.Sprintf("download_%d", time.Now().Unix())
}

// DefaultMaxConcurrentDownloads 默认同时下载任务数
const DefaultMaxConcurrentDownloads = 1

// GetMaxConcurrentDownloads 获取当前设置的最大同时下载任务数
func GetMaxConcurrentDownloads() int {
	var setting model.Setting
	if err := model.GetDB().First(&setting, 1).Error; err != nil {
		return DefaultMaxConcurrentDownloads
	}
	if setting.MaxConcurrentDownloads <= 0 {
		return DefaultMaxConcurrentDownloads
	}
	return setting.MaxConcurrentDownloads
}

// SetMaxConcurrentDownloads 设置最大同时下载任务数
func SetMaxConcurrentDownloads(n int) error {
	if n <= 0 {
		return fmt.Errorf("并发数必须大于0")
	}

	var setting model.Setting
	if err := model.GetDB().First(&setting, 1).Error; err != nil {
		setting = model.Setting{ID: 1, MaxConcurrentDownloads: n}
		return model.GetDB().Create(&setting).Error
	}

	setting.MaxConcurrentDownloads = n
	return model.GetDB().Save(&setting).Error
}

func StartTaskPolling(cfg *config.Config) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	log.Println("任务轮询服务已启动")
	for range ticker.C {
		tasks, err := GetActiveTasks()
		if err != nil {
			log.Printf("获取活跃任务失败: %v", err)
			continue
		}

		if len(tasks) > 0 {
			log.Printf("轮询: 发现 %d 个活跃任务", len(tasks))
		}

		var pendingTasks []model.Task
		downloadingCount := 0

		for _, task := range tasks {
			if task.Status == model.TaskStatusDownloading {
				log.Printf("轮询: 检查下载中任务 %d (PID: %d)", task.ID, task.PID)
				updateTaskStatus(task.ID)
				// 状态更新后重新确认是否仍在下载，已完成/失败的任务要释放并发槽位
				if updated, err := GetTaskByID(task.ID); err == nil && updated.Status == model.TaskStatusDownloading {
					downloadingCount++
				}
			} else if task.Status == model.TaskStatusPending {
				pendingTasks = append(pendingTasks, task)
			}
		}

		slots := GetMaxConcurrentDownloads() - downloadingCount
		for _, task := range pendingTasks {
			if slots <= 0 {
				break
			}
			log.Printf("轮询: 发现待处理任务 %d，启动下载", task.ID)
			go startDownloadTask(task.ID, cfg)
			slots--
		}
	}
}

func startDownloadTask(taskID uint, cfg *config.Config) {
	// 原子抢占：仅当任务仍处于 pending 时才置为 downloading，
	// 防止轮询间隔内同一任务被重复启动
	res := model.GetDB().Model(&model.Task{}).
		Where("id = ? AND status = ?", taskID, model.TaskStatusPending).
		Update("status", model.TaskStatusDownloading)
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}

	task, err := GetTaskByID(taskID)
	if err != nil {
		log.Printf("获取任务失败: %v", err)
		return
	}

	// 日志文件放在 downloads/Logs 目录下
	logDir := filepath.Join(cfg.DownloadDir, "Logs")
	os.MkdirAll(logDir, 0755)
	logFile := filepath.Join(logDir, fmt.Sprintf("task_%d.log", task.ID))
	task.PID = -1
	task.LogFile = logFile
	model.GetDB().Save(task)

	// 构建命令
	args := buildCommandArgs(task, cfg)

	// 打印命令用于调试
	logCmd := cfg.GetN_m3u8DLREPath() + " " + strings.Join(args, " ")
	log.Printf("执行命令: %s", logCmd)

	// 创建 context 用于超时控制
	ctx := context.Background()
	timeout := cfg.GetDownloadTimeout()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, cfg.GetN_m3u8DLREPath(), args...)
	// 容器环境默认没有 TERM，v0.6+ 的 tput 调用会因缺失而报错
	cmd.Env = os.Environ()
	if os.Getenv("TERM") == "" {
		cmd.Env = append(cmd.Env, "TERM=xterm")
	}
	logOut := openLogFile(logFile)
	cmd.Stdout = logOut
	cmd.Stderr = cmd.Stdout

	log.Printf("开始下载任务 %d: %s", task.ID, task.URL)

	if err := cmd.Start(); err != nil {
		updateTaskFailed(task.ID, err.Error())
		return
	}

	task.PID = cmd.Process.Pid
	model.GetDB().Save(task)

	if err := cmd.Wait(); err != nil {
		// 检查是否超时
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("任务 %d 超时", task.ID)
			// 终止进程
			cmd.Process.Kill()
		}
		log.Printf("任务 %d 退出错误: %v", task.ID, err)
	}

	// 关闭日志文件句柄（openLogFile 失败时回退为 os.Stdout，不能关）
	if logOut != os.Stdout {
		logOut.Close()
	}

	updateTaskStatus(task.ID)
}

func buildCommandArgs(task *model.Task, cfg *config.Config) []string {
	var args []string

	// URL 必须作为第一个参数（positional argument）
	args = append(args, task.URL)

	// 基本参数
	args = append(args, "--save-dir", cfg.DownloadDir)
	args = append(args, "--save-name", task.OutputName)

	// 线程数
	args = append(args, "--thread-count", strconv.Itoa(task.ThreadCount))

	// 重试次数
	args = append(args, "--download-retry-count", strconv.Itoa(task.RetryCount))

	// 请求头
	headers := parseHeaders(task.Headers)
	if strings.HasPrefix(task.URL, "https://surrit.com") && !hasHeader(headers, "Referer") {
		headers = append(headers, "Referer: https://missav.ws/")
	}
	for _, h := range headers {
		args = append(args, "-H", h)
	}

	// Base URL
	if task.BaseURL != "" {
		args = append(args, "--base-url", task.BaseURL)
	}

	// 自定义代理
	if task.CustomProxy != "" {
		args = append(args, "--custom-proxy", task.CustomProxy)
	}

	// 删除临时文件
	if task.DelAfterDone {
		args = append(args, "--del-after-done")
	} else {
		args = append(args, "--no-del-after-done")
	}

	// 二进制合并
	if task.BinaryMerge {
		args = append(args, "--binary-merge")
	}

	// 自动选择最佳视频和音频轨道（不选字幕，避免字幕 mux 进 mp4 失败；显式选流也避免多码率 master playlist 弹交互菜单导致无终端环境崩溃）
	if task.AutoSelect {
		args = append(args, "-sv", "best", "-sa", "best")
	}

	// 分片数量完整性检测（默认开启，勾选跳过后显式传 False）
	if task.SkipSegmentsCheck {
		args = append(args, "--check-segments-count", "False")
	}

	// 并行下载音视频（-mt）
	if task.ConcurrentDownload {
		args = append(args, "-mt")
	}
	// 下载完成后混流为 mp4（合并视频+音频，避免分离导致无声音）
	args = append(args, "-M", "format=mp4")

	// 解密
	if task.Key != "" {
		args = append(args, "--key", task.Key)
		args = append(args, "--decryption-engine", task.DecryptionEngine)
		if task.DecryptionEngine == "MP4DECRYPT" {
			args = append(args, "--decryption-binary-path", cfg.GetMp4decryptPath())
		}
	}

	// ffmpeg 路径
	args = append(args, "--ffmpeg-binary-path", cfg.GetFFmpegPath())

	// 自定义参数
	if task.CustomArgs != "" {
		customParts := strings.Split(task.CustomArgs, " ")
		for _, part := range customParts {
			part = strings.TrimSpace(part)
			if part != "" {
				args = append(args, part)
			}
		}
	}

	return args
}

func parseHeaders(rawHeaders string) []string {
	if rawHeaders == "" {
		return nil
	}

	var headers []string
	for _, h := range strings.Split(rawHeaders, ";") {
		h = strings.TrimSpace(h)
		if h != "" {
			headers = append(headers, h)
		}
	}

	return headers
}

func hasHeader(headers []string, headerName string) bool {
	prefix := strings.ToLower(headerName) + ":"
	for _, h := range headers {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(h)), prefix) {
			return true
		}
	}

	return false
}

// 任务日志检查时间阈值（秒）
// N_m3u8DL-RE 可能主进程退出但子进程继续工作
// 通过检查日志文件修改时间来判断任务是否还在进行
const logFileCheckThreshold = 30 // 30秒内有更新视为任务还在进行

func updateTaskStatus(taskID uint) {
	task, err := GetTaskByID(taskID)
	if err != nil {
		return
	}

	logFile := task.LogFile

	// 获取日志文件信息
	logFileInfo, err := os.Stat(logFile)
	if err != nil {
		return
	}

	// 检查日志文件是否在最近30秒内更新
	logFileMtime := logFileInfo.ModTime()
	logFileRecentlyUpdated := time.Since(logFileMtime) < logFileCheckThreshold*time.Second

	progressInfo := parseProgress(logFile)

	if progressInfo != nil {
		task.Progress = progressInfo.Progress
		task.Speed = progressInfo.Speed
		task.DownloadedSize = progressInfo.DownloadedSize
		task.TotalSize = progressInfo.TotalSize
	}

	// 真正检测进程是否还在运行
	processStillRunning := task.PID > 0 && isProcessAlive(task.PID)

	// 进程还在运行：保存进度并保留真实 PID，以便删除任务时能正确检测并终止进程
	if processStillRunning {
		model.GetDB().Save(task)
		return
	}

	// 进程已不存在，但如果日志最近有更新，说明子进程还在工作
	if task.Status == model.TaskStatusDownloading {
		if logFileRecentlyUpdated {
			// 日志最近有更新，检查是否已完成
			if content, _, err := readLogTail(logFile, 256*1024); err == nil {
				if strings.Contains(content, "合并完成") || strings.Contains(content, "downloaded successfully") || strings.Contains(content, " Done") {
					task.Status = model.TaskStatusCompleted
					now := time.Now()
					task.FinishedAt = &now
					task.Progress = 100
					task.PID = 0
					model.GetDB().Save(task)
					return
				}
			}
			// 日志还在更新，任务可能正在进行（主进程退出但子进程在工作）
			task.PID = 0
			model.GetDB().Save(task)
			return
		}
		// 进程已死且日志也没有更新，标记为中断
		task.Status = model.TaskStatusInterrupted
		task.ErrorMsg = "下载进程已中断，请重试或删除任务"
		now := time.Now()
		task.FinishedAt = &now
		task.PID = 0
		model.GetDB().Save(task)
		return
	}

	// 进程已结束，检查日志更新状态
	if content, _, err := readLogTail(logFile, 256*1024); err == nil {
		// 检查完成状态
		if strings.Contains(content, "合并完成") || strings.Contains(content, "downloaded successfully") || strings.Contains(content, " Done") {
			task.Status = model.TaskStatusCompleted
			now := time.Now()
			task.FinishedAt = &now
			task.Progress = 100
		} else {
			// 检查真正的错误（排除警告）
			errorRe := regexp.MustCompile("(?i)(ERROR|Error|exception|Exception|失败|异常)[^`]*")
			if matches := errorRe.FindStringSubmatch(content); len(matches) > 0 {
				// 排除一些误匹配的警告
				if !strings.Contains(matches[0], "start time") && !strings.Contains(matches[0], "timestamp discontinuity") {
					task.Status = model.TaskStatusFailed
					task.ErrorMsg = matches[0]
					now := time.Now()
					task.FinishedAt = &now
				}
			}
		}
	}

	task.PID = 0
	model.GetDB().Save(task)
}

func updateTaskFailed(taskID uint, errorMsg string) {
	task, _ := GetTaskByID(taskID)
	if task != nil {
		task.Status = model.TaskStatusFailed
		task.ErrorMsg = errorMsg
		now := time.Now()
		task.FinishedAt = &now
		model.GetDB().Save(task)
	}
}

// ProgressInfo 下载进度信息
type ProgressInfo struct {
	Progress       int
	Speed          string
	DownloadedSize string
	TotalSize      string
}

// cleanANSI 清理ANSI转义码和特殊Unicode字符
func cleanANSI(s string) string {
	// 使用正则清理 ANSI 转义码
	ansiRe := regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	s = ansiRe.ReplaceAllString(s, "")

	// 清理进度条Unicode字符（━ U+2501）
	barRe := regexp.MustCompile(`[━]{10,}`)
	s = barRe.ReplaceAllString(s, " ")

	return s
}

func parseProgress(logFile string) *ProgressInfo {
	// 进度行在日志末尾，只读尾部避免长日志全量读入
	content, _, err := readLogTail(logFile, 64*1024)
	if err != nil {
		return nil
	}

	// v0.6+ 重定向日志无换行，先规整切分再按行扫描
	lines := strings.Split(normalizeLogContent(content), "\n")
	result := &ProgressInfo{}

	// 只从最后几行查找视频轨道进度
	// v0.5 格式: Vid Kbps 1268/1735 73.08% 913.30MB/1.22GB 3.10MBps 00:01:09
	// v0.6+ 格式: Vid 1920x1080 | 2790 Kbps | ... ━━━ 25/49 51.02%450.20MB 2.10MBps 00:00:20
	for i := len(lines) - 1; i >= 0; i-- {
		line := cleanANSI(lines[i])
		line = strings.TrimSpace(line)

		// 跳过空行和非视频轨道行（Aud/Sub 行不代表整体进度）
		if line == "" || !strings.HasPrefix(line, "Vid") {
			continue
		}

		// 解析进度百分比
		progressRe := regexp.MustCompile(`(\d+\.?\d*)%`)
		if matches := progressRe.FindStringSubmatch(line); len(matches) > 1 {
			// 使用 ParseFloat 因为进度可能是浮点数
			if p, err := strconv.ParseFloat(matches[1], 64); err == nil {
				result.Progress = int(p)
				if result.Progress > 100 {
					result.Progress = 100
				}
				if result.Progress < 0 {
					result.Progress = 0
				}
			}
		}

		// 解析下载大小 - 格式: 913.30MB/1.22GB
		sizeRe := regexp.MustCompile(`(\d+\.?\d*\s*[KMG]?B)/(\d+\.?\d*\s*[KMG]?B)`)
		if sizeMatches := sizeRe.FindStringSubmatch(line); len(sizeMatches) > 2 {
			result.DownloadedSize = sizeMatches[1]
			result.TotalSize = sizeMatches[2]
		}

		// 解析速度 - 格式: 3.10MBps 或 1.5GB/s (在时间前面的那个)
		speedRe := regexp.MustCompile(`(\d+\.?\d*\s*[KMG]?B(?:ps|/s)?)\s+\d{2}:\d{2}:\d{2}$`)
		if speedMatches := speedRe.FindStringSubmatch(line); len(speedMatches) > 1 {
			result.Speed = speedMatches[1]
		}

		// 如果没有精确匹配到速度，尝试其他方式
		if result.Speed == "" {
			// 查找所有速度格式的匹配
			speedRe := regexp.MustCompile(`(\d+\.?\d*\s*[KMG]?B(?:ps|/s)?)`)
			if speedMatches := speedRe.FindAllStringSubmatch(line, -1); len(speedMatches) >= 2 {
				// 第二个匹配通常是速度（在已下载/总大小后面）
				result.Speed = speedMatches[1][1]
			}
		}

		// 找到进度行就退出
		if result.Progress > 0 || result.DownloadedSize != "" {
			break
		}
	}

	return result
}

func openLogFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("创建日志文件失败: %v", err)
		return os.Stdout
	}
	return file
}

// readLogTail 读取日志文件末尾 maxBytes 字节，避免长日志全量读入内存。
// truncated 为 true 时表示内容被截断，不完整的首行已被丢弃。
func readLogTail(path string, maxBytes int64) (string, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", false, err
	}

	offset := int64(0)
	if info.Size() > maxBytes {
		offset = info.Size() - maxBytes
	}

	buf := make([]byte, info.Size()-offset)
	if _, err := f.ReadAt(buf, offset); err != nil && err != io.EOF {
		return "", false, err
	}

	truncated := offset > 0
	if truncated {
		// 只在开头小窗口内找换行符对齐首行；
		// 日志以稀疏换行格式写入时（如 v0.6+ 重定向输出）避免把全部内容丢弃
		window := string(buf)
		if len(window) > 4096 {
			window = window[:4096]
		}
		if idx := strings.IndexByte(window, '\n'); idx != -1 {
			buf = buf[idx+1:]
		}
	}
	return string(buf), truncated, nil
}

// readLogHeadLines 读取日志文件开头的 maxLines 行。
// 先读取开头 64KB（足够覆盖前 50 行），再用 normalizeLogContent 按行切分，
// 兼容 v0.5（有换行）与 v0.6+（重定向无换行）两种日志格式。
func readLogHeadLines(path string, maxLines int) (string, error) {
	const headBytes = 128 * 1024

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", err
	}

	size := int64(headBytes)
	if info.Size() < size {
		size = info.Size()
	}
	buf := make([]byte, size)
	if _, err := f.Read(buf); err != nil && err != io.EOF {
		return "", err
	}

	lines := strings.Split(normalizeLogContent(string(buf)), "\n")
	if len(lines) > maxLines {
		lines = lines[:maxLines]
	}
	return strings.Join(lines, "\n"), nil
}

// readLogTailLines 读取日志文件末尾的 maxLines 行。
// 先读取末尾 128KB（足够覆盖最后 100 行），再用 normalizeLogContent 按行切分，
// 兼容 v0.5（有换行）与 v0.6+（重定向无换行）两种日志格式。
func readLogTailLines(path string, maxLines int) (string, error) {
	const tailBytes = 128 * 1024

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", err
	}

	size := int64(tailBytes)
	if info.Size() < size {
		size = info.Size()
	}
	buf := make([]byte, size)
	offset := info.Size() - size
	if _, err := f.ReadAt(buf, offset); err != nil && err != io.EOF {
		return "", err
	}

	lines := strings.Split(normalizeLogContent(string(buf)), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n"), nil
}

var (
	// 日志条目起始模式，如 "18:43:35.061 WARN"
	logEntryRe = regexp.MustCompile(`(\d{2}:\d{2}:\d{2}\.\d{3}\s+(?:INFO|WARN|ERROR|VERB))`)
	// v0.6+ 进度表格行起始模式，如 "Sub en | English"、"Vid 1920x1080 | 2790 Kbps"
	logTableRowRe  = regexp.MustCompile(`((?:Sub|Vid|Aud)\s+\S[^\n|]*\s\|\s)`)
	multiNewlineRe = regexp.MustCompile(`\n{2,}`)
)

// normalizeLogContent 把无换行的日志（v0.6+ 重定向输出）切分为可读的行；
// 对本身有换行的日志处理后内容不变
func normalizeLogContent(s string) string {
	s = logEntryRe.ReplaceAllString(s, "\n$1")
	s = logTableRowRe.ReplaceAllString(s, "\n$1")
	s = multiNewlineRe.ReplaceAllString(s, "\n")
	return strings.TrimLeft(s, "\n")
}
