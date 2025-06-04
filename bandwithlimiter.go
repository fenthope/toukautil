package toukautil

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/infinite-iroha/touka"
	"golang.org/x/time/rate"
)

// BandwidthLimiterOptions 用于配置带宽限制中间件。
type BandwidthLimiterOptions struct {
	// UploadLimit 是针对每个 key 的上传（请求体读取）速率限制 (Bytes/s)。
	// rate.Inf 表示无独立上传限制。
	UploadLimit rate.Limit
	// UploadBurst 是上传令牌桶的突发容量 (bytes)。
	UploadBurst int

	// DownloadLimit 是针对每个 key 的下载（响应体写入）速率限制 (Bytes/s)。
	// rate.Inf 表示无独立下载限制。
	DownloadLimit rate.Limit
	// DownloadBurst 是下载令牌桶的突发容量 (bytes)。
	DownloadBurst int

	// KeyFunc 用于从 Touka Context 中为每个请求提取一个唯一的字符串 key。
	// 上传和下载将使用此 key 应用独立的速率限制器。
	// 如果为 nil，默认不进行基于 key 的独立限速（只受全局限速影响，如果全局限速开启）。
	// 如果希望基于IP限速，可以设置为: func(c *touka.Context) string { return c.ClientIP() }
	KeyFunc func(c *touka.Context) string

	// Skip 是一个可选函数，如果返回 true，则当前请求将跳过所有带宽限制检查。
	Skip func(c *touka.Context) bool
}

// defaultBandwidthKeyFunc 是一个示例，如果需要默认基于IP的key。
// 但为了更明确，我们将让用户在需要时提供 KeyFunc。
// func defaultBandwidthKeyFunc(c *touka.Context) string {
// 	ip := c.ClientIP()
// 	if ip == "" { return "touka-bandwidth-unknown-ip" }
// 	return ip
// }

// BandwidthLimit 返回一个 Touka 中间件，用于限制请求体上传和响应体下载的带宽。
// 它会同时考虑通过 SetGlobalRateLimit/SetGlobalWriteRateLimit 设置的全局限制
// 以及通过 BandwidthLimiterOptions 配置的针对每个 key 的独立限制。
func BandwidthLimit(opts BandwidthLimiterOptions) touka.HandlerFunc {
	return func(c *touka.Context) {
		// 1. 检查是否应跳过此请求的带宽限制
		if opts.Skip != nil && opts.Skip(c) {
			c.Next()
			return
		}

		var key string
		if opts.KeyFunc != nil {
			key = opts.KeyFunc(c)
			// 如果 key 为空，我们可能仍然希望应用全局限制（如果开启），
			// 或者跳过独立限制。为简单起见，如果 KeyFunc 返回空，我们不应用独立限制。
			// if key == "" {
			// log.Println("touka.BandwidthLimit: Warning - KeyFunc returned empty string. No key-specific limits applied.")
			// }
		}

		// 2. 处理上传限速 (请求体读取)
		// 只有当配置了有效的独立上传限制时，或者全局读取限制激活时，才包装请求体。
		// RateLimitedReader 内部会处理全局限制。
		// 我们需要知道全局读限制是否激活。可以直接检查 globalLimiter.Load().Limit()
		currentGlobalReadLimiter := globalLimiter.Load() // 从 limitreader.go 导入的全局变量
		applyUploadLimit := (opts.UploadLimit > 0 && opts.UploadLimit != rate.Inf && key != "") ||
			(currentGlobalReadLimiter.Limit() != rate.Inf)

		if applyUploadLimit && c.Request.Body != nil && c.Request.Body != http.NoBody {
			// 如果 KeyFunc 返回空字符串，则独立限制的 limit 和 burst 将是 Inf 和 0，
			// 这样 RateLimitedReader 就只会受到全局限速的影响。
			var individualUploadLimit rate.Limit = rate.Inf
			var individualUploadBurst int = 0
			if key != "" && opts.UploadLimit > 0 && opts.UploadLimit != rate.Inf {
				individualUploadLimit = opts.UploadLimit
				individualUploadBurst = opts.UploadBurst
			}

			// 使用请求的 Context 来创建 RateLimitedReader
			// RateLimitedReader 会自动处理全局读取限制 (globalLimiter)
			limitedReqBody := NewRateLimitedReader(
				c.Request.Body,
				individualUploadLimit,
				individualUploadBurst,
				c.Request.Context(), // 传递请求上下文
			)
			// 替换原始请求体
			originalReqBody := c.Request.Body
			c.Request.Body = limitedReqBody

			// 确保在请求处理完毕后关闭我们创建的 RateLimitedReader (它会关闭原始的Body)
			// Touka 的 Context 在每个请求后会重置，通常不需要我们显式恢复 c.Request.Body
			// 但如果原始 Body 需要特殊关闭，这里可以处理
			defer func() {
				// RateLimitedReader.Close() 会调用原始 Body 的 Close (如果它是 io.Closer)
				// 如果原始 Body 不是 Closer，则无操作。
				// 确保我们不会重复关闭一个已经被后续处理器关闭的 Body。
				// 通常 HTTP Server 会在读取完 Body 或请求结束时关闭它。
				// 为了安全，显式关闭我们包装的 reader。
				if err := limitedReqBody.Close(); err != nil {
					// 使用英文记录日志
					// log.Printf("touka.BandwidthLimit: Error closing rate-limited request body: %v", err)
					c.AddError(fmt.Errorf("closing rate-limited request body: %w", err))
				}
				// 恢复原始Body指针（如果后续有逻辑需要它，虽然通常不必要）
				// c.Request.Body = originalReqBody
				_ = originalReqBody // 避免 unused 警告，如果上面恢复注释掉
			}()
		}

		// 3. 处理下载限速 (响应体写入)
		// 与上传类似，只有当配置了有效的独立下载限制或全局写入限制激活时才包装 Writer。
		currentGlobalWriteLimiter := globalWriterLimiter.Load() // 从 limitwriter.go 导入的全局变量
		applyDownloadLimit := (opts.DownloadLimit > 0 && opts.DownloadLimit != rate.Inf && key != "") ||
			(currentGlobalWriteLimiter.Limit() != rate.Inf)

		var originalClientWriter touka.ResponseWriter // Touka 的 ResponseWriter 接口
		if applyDownloadLimit {
			var individualDownloadLimit rate.Limit = rate.Inf
			var individualDownloadBurst int = 0
			if key != "" && opts.DownloadLimit > 0 && opts.DownloadLimit != rate.Inf {
				individualDownloadLimit = opts.DownloadLimit
				individualDownloadBurst = opts.DownloadBurst
			}

			originalClientWriter = c.Writer // 保存原始 Writer
			// RateLimitedWriter 会自动处理全局写入限制 (globalWriterLimiter)
			limitedClientWriter := NewRateLimitedWriter(
				originalClientWriter, // 包装原始的 ResponseWriter
				individualDownloadLimit,
				individualDownloadBurst,
				c.Request.Context(), // 传递请求上下文
			)
			c.Writer = limitedClientWriter // 替换 Context 中的 Writer
		}

		// 4. 调用链中的下一个处理器
		c.Next()

		// 5. 清理 (如果替换了 Writer)
		// 在 c.Next() 返回后，如果 c.Writer 被替换了，
		// 我们不需要显式地将 c.Writer 恢复为 originalClientWriter，
		// 因为 c.Writer 是请求作用域的，在下一个请求中它会被重置。
		// RateLimitedWriter 如果实现了 io.Closer (它会代理给底层的 Closer)，
		// 并且底层 Writer 需要关闭，则可能需要在这里关闭。
		// 但 http.ResponseWriter 通常不需要显式关闭。
		if applyDownloadLimit && originalClientWriter != nil {
			// 如果 RateLimitedWriter 本身需要清理（例如它内部有定时器等），可以在这里处理。
			// 当前的 RateLimitedWriter 实现没有这样的需求。
			// log.Printf("RateLimitedWriter was active for %s", c.Request.URL.Path)
		}
	}
}

// --- 字符串速率解析函数 ---

var (
	// rateRegex 匹配速率字符串，捕获数值和单位
	rateRegex = regexp.MustCompile(`^([\d.]+)\s*([a-zA-Z/]*)$`)

	// unitToBytesPerSec 将单位字符串映射到转换为 Bytes/s 的乘数 (小写单位为键)
	unitToBytesPerSec = map[string]float64{
		// 比特单位 (转换为字节，除以 8)
		"bps":  1.0 / 8.0,
		"kbps": 1000.0 / 8.0,
		"mbps": 1000.0 * 1000.0 / 8.0,
		"gbps": 1000.0 * 1000.0 * 1000.0 / 8.0,

		// 字节单位 (1024-based)
		"b":       1.0, // 假设是 B/s (Bytes/s)
		"":        1.0, // 没有单位也假设是 B/s
		"b/s":     1.0,
		"byte":    1.0,
		"bytes/s": 1.0,

		"k":           1024.0, // 缩写 KB/s
		"kb":          1024.0,
		"kb/s":        1024.0,
		"kilobyte":    1024.0,
		"kilobytes/s": 1024.0,

		"m":           1024.0 * 1024.0, // 缩写 MB/s
		"mb":          1024.0 * 1024.0,
		"mb/s":        1024.0 * 1024.0,
		"megabyte":    1024.0 * 1024.0,
		"megabytes/s": 1024.0 * 1024.0,

		"g":           1024.0 * 1024.0 * 1024.0, // 缩写 GB/s
		"gb":          1024.0 * 1024.0 * 1024.0,
		"gb/s":        1024.0 * 1024.0 * 1024.0,
		"gigabyte":    1024.0 * 1024.0 * 1024.0,
		"gigabytes/s": 1024.0 * 1024.0 * 1024.0,
	}
)

const UnlimitedRateString = "-1"
const UnDefiendRateString = "0"

// ParseRate 解析人类可读的速度字符串 (例如, "100kbps", "1.5MB/s", "5000")。
// 返回速率，单位是每秒字节数 (rate.Limit)。
// 如果 rateStr 为 "-1" (不区分大小写，忽略空格)，则返回 rate.Inf 表示无限速。
// 如果解析结果为非正数 (且不是 "-1")，则返回错误。
func ParseRate(rateStr string) (rate.Limit, error) {
	rateStr = strings.TrimSpace(rateStr)

	// 特殊处理无限速字符串
	if rateStr == UnlimitedRateString {
		return rate.Inf, nil
	}

	// 处理未定义
	if rateStr == UnDefiendRateString {
		// 返回 UnDefiendRateStringErr 类型的错误
		return rate.Inf, &UnDefiendRateStringErr{
			s: "rate string cannot be 0, for unlimited should use -1",
		}
	}

	if rateStr == "" {
		// 空字符串被视为无效输入，而不是无限速
		return 0, fmt.Errorf("rate string cannot be empty")
	}

	match := rateRegex.FindStringSubmatch(rateStr)
	if len(match) < 3 {
		// rateRegex 期望至少匹配一个数值部分
		return 0, fmt.Errorf("invalid rate format: %s", rateStr)
	}

	valueStr := match[1]                 // 数值部分 (例如 "100" 或 "1.5")
	unitStr := strings.ToLower(match[2]) // 单位部分，转为小写 (例如 "kbps" 或 "mb/s")

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in rate '%s': %w", rateStr, err)
	}

	// 查找单位对应的乘数
	multiplier, ok := unitToBytesPerSec[unitStr]
	if !ok {
		return 0, fmt.Errorf("unknown or unsupported rate unit: '%s' in '%s'", match[2], rateStr)
	}

	bytesPerSecond := value * multiplier

	// 确保计算出的速率是正数。0 或负数速率被视为无效。
	if bytesPerSecond <= 0 {
		return 0, fmt.Errorf("calculated rate is non-positive (%.2f B/s) from '%s'", bytesPerSecond, rateStr)
	}

	return rate.Limit(bytesPerSecond), nil
}
