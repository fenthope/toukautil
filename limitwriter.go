// Copyright 2025 WJQSERVER-STUDIO. All rights reserved.
// Use of this source code is governed by WSL
// license that can be found in the LICENSE.WSL file.
// Copyright 2025 Infinite-Iroha Group, WJQSERVER. All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.

package toukautil

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"sync/atomic"

	"github.com/infinite-iroha/touka"
	"golang.org/x/time/rate"
)

// --- 全局写入限速器 ---
var (
	// globalWriterLimiter 是全局写入速率限速器。
	// 使用 atomic.Pointer 来实现对 *rate.Limiter 的原子读写。
	// 重要假设: globalWriterLimiter 在初始化设置后，其 Limit 在运行时不再通过 SetGlobalWriteRateLimit 改变。
	globalWriterLimiter atomic.Pointer[rate.Limiter]
)

func init() {
	// 包初始化时，设置一个默认的无限速全局写入限速器。
	globalWriterLimiter.Store(rate.NewLimiter(rate.Inf, 0))
}

// SetGlobalWriteRateLimit 设置全局写入速率限制。
// limit: 全局写入速率限制，单位是 Bytes/s (rate.Limit)。rate.Inf 表示无限制。
// burst: 全局写入令牌桶的突发容量，单位是字节。
// 将 limit 设置为 <= 0 或 rate.Inf 将禁用全局写入限速。
// 重要提示: 此函数应主要在应用程序初始化期间调用。
// 在 RateLimitedWriter 实例创建后更改全局限制可能不会反映在这些实例的缓存状态中。
func SetGlobalWriteRateLimit(limit rate.Limit, burst int) {
	var newLimiter *rate.Limiter
	if limit <= 0 || limit == rate.Inf {
		newLimiter = rate.NewLimiter(rate.Inf, 0) // 无限速
	} else {
		newLimiter = rate.NewLimiter(limit, burst)
	}
	globalWriterLimiter.Store(newLimiter) // 原子地更新全局限速器
}

// --- 限速写入器 RateLimitedWriter ---

// RateLimitedWriter 包装一个 io.Writer (通常是 http.ResponseWriter)，并对其写入操作应用速率限制。
// 它同时受其自身配置的独立限速器和（如果激活）全局写入限速器的约束。
type RateLimitedWriter struct {
	w       io.Writer       // 原始写入器 (例如: http.ResponseWriter, os.File)
	limiter *rate.Limiter   // 此写入器实例的独立令牌桶限速器
	ctx     context.Context // 与此写入操作关联的 Context，用于取消等待

	// 缓存的限速状态，在创建 RateLimitedWriter 实例时确定。
	// 这些状态基于全局限速器在创建时配置不变的假设。
	globalLimitActiveAtCreation     bool // 创建时全局写入限速是否开启
	individualLimitActiveAtCreation bool // 创建时独立写入限速是否开启
	bypassLimiting                  bool // 如果创建时全局和独立限速都未激活，则为 true，可完全跳过限速逻辑
}

// NewRateLimitedWriter 创建一个新的 RateLimitedWriter 实例。
// w: 底层的 io.Writer。
// limit: 此写入器的独立速率限制，单位 Bytes/s (rate.Limit)。rate.Inf 表示无独立限制。
// burst: 此写入器的独立令牌桶突发容量，单位字节。
// ctx: 与写入操作关联的 Context，用于在等待令牌时支持取消。
//
// 注意: 全局写入限速器的状态是在调用此构造函数时确定的，并缓存。
// 如果在 RateLimitedWriter 实例存在期间通过 SetGlobalWriteRateLimit 更改全局限制，
// 已创建的实例将继续使用其创建时缓存的全局限速状态。
func NewRateLimitedWriter(w io.Writer, limit rate.Limit, burst int, ctx context.Context) *RateLimitedWriter {
	if ctx == nil {
		ctx = context.Background() // 如果未提供上下文，则使用默认的后台上下文
	}

	// 创建并配置独立限速器
	var individualLimiter *rate.Limiter
	individualLimitActive := false
	if limit > 0 && limit != rate.Inf {
		individualLimiter = rate.NewLimiter(limit, burst)
		individualLimitActive = true
	} else {
		individualLimiter = rate.NewLimiter(rate.Inf, 0) // 无独立限制
	}

	// 获取创建时全局写入限速器的状态
	currentGlobalWriterLimiter := globalWriterLimiter.Load() // 原子加载
	globalLimitActive := currentGlobalWriterLimiter.Limit() != rate.Inf

	// 判断是否可以完全绕过限速逻辑
	bypass := !globalLimitActive && !individualLimitActive

	return &RateLimitedWriter{
		w:       w,
		limiter: individualLimiter,
		ctx:     ctx,

		globalLimitActiveAtCreation:     globalLimitActive,
		individualLimitActiveAtCreation: individualLimitActive,
		bypassLimiting:                  bypass,
	}
}

// Write 实现 io.Writer 接口。
// 它在将数据写入底层写入器之前，根据配置的速率限制等待必要的令牌。
// 写入的字节数 (n) 用于从限速器请求相应数量的令牌。
func (rlw *RateLimitedWriter) Write(p []byte) (n int, err error) {
	bytesToWrite := len(p)
	if bytesToWrite == 0 {
		// 尝试写入0字节时，不消耗令牌，直接调用底层 Write (如果有)。
		// 有些 Writer 实现可能对0字节写入有特定行为。
		return rlw.w.Write(p)
	}

	// 检查是否可以完全绕过限速（基于创建时的状态）
	if rlw.bypassLimiting {
		return rlw.w.Write(p) // 直接透穿写入
	}

	// 如果执行到这里，说明创建时至少有一个限速器是激活的。

	// 1. 应用全局写入限速 (如果创建时是激活的)
	if rlw.globalLimitActiveAtCreation {
		currentGlobalLimiter := globalWriterLimiter.Load() // 再次原子加载，以获取最新的指针（尽管Limit值不变）
		// WaitN 会阻塞，直到有足够令牌或上下文被取消
		if waitErr := currentGlobalLimiter.WaitN(rlw.ctx, bytesToWrite); waitErr != nil {
			// 如果等待被上下文取消或发生其他错误，则返回0字节写入和该错误
			return 0, waitErr
		}
	}

	// 2. 应用独立写入限速 (如果创建时是激活的)
	if rlw.individualLimitActiveAtCreation {
		// WaitN 会阻塞，直到有足够令牌或上下文被取消
		if waitErr := rlw.limiter.WaitN(rlw.ctx, bytesToWrite); waitErr != nil {
			// 如果等待被上下文取消或发生其他错误，则返回0字节写入和该错误
			return 0, waitErr
		}
	}

	// 令牌获取成功（或无需令牌），执行实际的写入操作
	n, err = rlw.w.Write(p)
	// Write 方法不应该修改 p，所以不需要担心 WaitN 之后 p 的内容改变。
	// n 返回的是实际写入的字节数，这可能小于 len(p) 而不报错。
	// 限速器是基于尝试写入的字节数 (bytesToWrite) 来消耗令牌的。
	// 如果实际写入的字节数 n < bytesToWrite 但没有错误，这通常表示部分写入，
	// 但我们已经为全部 bytesToWrite "支付"了令牌。这是 rate.Limiter 的标准行为：
	// 你为意图的操作量请求许可。

	return n, err
}

// Close 实现 io.Closer 接口 (如果底层写入器支持)。
// 它会将 Close 调用传递给包装的 io.Writer (如果它实现了 io.Closer)。
func (rlw *RateLimitedWriter) Close() error {
	if closer, ok := rlw.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil // 如果底层 Writer 不是 Closer，则 Close 是无操作
}

// Flush 实现 http.Flusher 接口 (如果底层写入器支持)。
// 这对于像 http.ResponseWriter 这样的写入器很重要，可以确保数据被发送到客户端。
// 注意：Flush 操作本身不应该被速率限制，因为它只是刷新已写入的缓冲数据。
// 速率限制应用于 Write 操作。
func (rlw *RateLimitedWriter) Flush() {
	if flusher, ok := rlw.w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Hijack 实现 http.Hijacker 接口 (如果底层写入器支持)。
// 对于需要接管连接的场景 (如 WebSocket)，代理或包装器需要支持 Hijack。
// 速率限制不适用于劫持后的连接，因为协议已改变。
func (rlw *RateLimitedWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := rlw.w.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	// 使用英文返回错误
	return nil, nil, fmt.Errorf("RateLimitedWriter: underlying writer %T does not implement http.Hijacker", rlw.w)
}

// Header 实现 http.ResponseWriter 接口的部分功能 (如果底层写入器是 http.ResponseWriter)。
// 这使得 RateLimitedWriter 在包装 http.ResponseWriter 时能更透明。
func (rlw *RateLimitedWriter) Header() http.Header {
	if hrw, ok := rlw.w.(http.ResponseWriter); ok {
		return hrw.Header()
	}
	// 如果底层不是 http.ResponseWriter，返回 nil 或一个空的 Header Map
	// 通常，如果期望 Header()，则底层 Writer 应该是 http.ResponseWriter
	return nil
}

// WriteHeader 实现 http.ResponseWriter 接口的部分功能。
func (rlw *RateLimitedWriter) WriteHeader(statusCode int) {
	if hrw, ok := rlw.w.(http.ResponseWriter); ok {
		hrw.WriteHeader(statusCode)
	}
}

// Status 返回写入的 HTTP 状态码。
// 它尝试调用底层 Writer 的 Status() 方法（如果它是 touka.ResponseWriter）。
// 否则，它无法确定状态码，可以返回0或一个默认值。
func (rlw *RateLimitedWriter) Status() int {
	if trw, ok := rlw.w.(touka.ResponseWriter); ok { // ResponseWriter 是 touka.ResponseWriter 接口
		return trw.Status()
	}
	// 如果底层不是 Touka 的 ResponseWriter，我们没有简单的方法获取 Status。
	// Touka 的 responseWriterImpl 内部有 status 字段。
	// 如果 RateLimitedWriter 自己也需要跟踪状态（当 WriteHeader 被调用时），
	// 就需要增加一个 status 字段并在此处返回。
	// 为简单起见，如果底层不支持，返回0（表示未写入或未知）。
	return 0 // 或者 http.StatusOK 如果 WriteHeader 被调用过且没有特定状态
}

// Size 返回已写入响应体的字节数。
// 它尝试调用底层 Writer 的 Size() 方法。
// 注意：这个 Size 反映的是通过底层 Writer 实际写入的字节数，
// RateLimitedWriter 本身不直接修改或计算这个值。
func (rlw *RateLimitedWriter) Size() int {
	if trw, ok := rlw.w.(touka.ResponseWriter); ok { // ResponseWriter 是 touka.ResponseWriter 接口
		return trw.Size()
	}
	return 0 // 如果底层不支持，返回0
}

// Written 返回 WriteHeader 是否已被调用。
// 它尝试调用底层 Writer 的 Written() 方法。
func (rlw *RateLimitedWriter) Written() bool {
	if trw, ok := rlw.w.(touka.ResponseWriter); ok { // ResponseWriter 是 touka.ResponseWriter 接口
		return trw.Written()
	}
	// 如果底层不是 Touka 的 ResponseWriter，
	// 我们可以尝试从 http.ResponseWriter 的角度判断，但这不完全等价。
	// 或者，如果 RateLimitedWriter 自身也跟踪 WriteHeader 调用，可以在这里反映。
	// 为简单起见，如果底层不支持，返回 false（或一个基于自身状态的判断）。
	return false // 无法确定，或假设未写入
}

// 以下函数用于从你的 limitreader 包中解析速率字符串。
// 你需要确保这些函数在你的项目中是可访问的 (例如，将它们移动到共享的工具包，
// 或者如果 RateLimitedWriter 和 RateLimitedReader 在同一个包中，则可以直接使用)。
// 为了完整性，我将它们复制过来，并假设它们在同一个包或可访问。

const UnDefinedRateString = "0" // 无效/未定义速率的字符串表示

// UnDefinedRateErr 表示速率字符串为 "0"，这被认为是一个无效的定义。
// 无限速率应使用 "-1"。
type UnDefinedRateErr struct {
	s string
}

func (e *UnDefinedRateErr) Error() string {
	return e.s
}

/*
// ParseRate 解析人类可读的速率字符串 (例如 "100kbps", "1.5MB/s")。
// 返回速率 (rate.Limit, Bytes/s) 和错误。
// "-1" 表示无限速率 (rate.Inf)。"0" 被视为无效并返回 UnDefinedRateErr。
func ParseRate(rateStr string) (rate.Limit, error) {
	rateStr = strings.TrimSpace(rateStr)
	if rateStr == UnlimitedRateString {
		return rate.Inf, nil
	}
	if rateStr == UnDefinedRateString {
		return 0, &UnDefinedRateErr{s: "rate string '0' is undefined; use '-1' for unlimited rate"}
	} // 英文错误
	if rateStr == "" {
		return 0, fmt.Errorf("rate string cannot be empty")
	} // 英文错误

	match := rateRegex.FindStringSubmatch(rateStr)
	if len(match) < 3 {
		return 0, fmt.Errorf("invalid rate format: %s", rateStr)
	} // 英文错误

	valueStr := match[1]
	unitStr := strings.ToLower(match[2])
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in rate '%s': %w", rateStr, err)
	} // 英文错误

	multiplier, ok := unitToBytesPerSec[unitStr]
	if !ok {
		return 0, fmt.Errorf("unknown rate unit '%s' in '%s'", match[2], rateStr)
	} // 英文错误

	bytesPerSecond := value * multiplier
	if bytesPerSecond <= 0 {
		return 0, fmt.Errorf("calculated rate is non-positive (%.2f B/s) from '%s'", bytesPerSecond, rateStr)
	} // 英文错误
	return rate.Limit(bytesPerSecond), nil
}
*/
