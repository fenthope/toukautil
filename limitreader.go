// Copyright 2025 WJQSERVER-STUDIO. All rights reserved.
// Use of this source code is governed by WSL
// license that can be found in the LICENSE.WSL file.

package toukautil

import (
	"context"
	"io"
	"sync/atomic" // 引入 atomic 包

	"golang.org/x/time/rate"
)

// --- 全局限速器 ---

var (
	// globalLimiter 是全局读取速率限速器。
	// 使用 atomic.Pointer 来实现对 *rate.Limiter 的原子读写。
	// !! 重要假设: globalLimiter 在初始化设置后，其 Limit 在运行时不再通过 SetGlobalRateLimit 改变。
	globalLimiter atomic.Pointer[rate.Limiter]
)

func init() {
	// 在包初始化时设置初始的无限速全局限速器
	globalLimiter.Store(rate.NewLimiter(rate.Inf, 0))
}

// SetGlobalRateLimit 设置全局读取速率限制。
// limit: 全局速率限制，单位是 Bytes/s (rate.Limit)。rate.Inf 表示无限制。
// burst: 全局令牌桶的突发容量，单位是字节。
// 将 limit 设置为 <= 0 或 rate.Inf 将禁用全局限速。
// !! 重要提示: 此函数应仅在应用程序初始化期间调用。
// !! 在 RateLimitedReader 实例创建后更改全局限制将不会反映在这些实例的缓存状态中。
func SetGlobalRateLimit(limit rate.Limit, burst int) {
	var newLimiter *rate.Limiter
	// 如果 limit 非正或为 Inf，则创建无限速的全局限速器
	if limit <= 0 || limit == rate.Inf {
		newLimiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		newLimiter = rate.NewLimiter(limit, burst)
	}
	// 原子地存储新的 limiter 指针，替换旧的
	globalLimiter.Store(newLimiter)
}

// --- 限速读取器 RateLimitedReader ---

// RateLimitedReader 包装一个 io.Reader，并应用速率限制。
// 它同时受自身独立限速器和全局限速器的约束。
type RateLimitedReader struct {
	r       io.Reader       // 原始读取器 (如: resp.Body)
	limiter *rate.Limiter   // 独立令牌桶限速器
	ctx     context.Context // 用于取消等待的 Context (通常是请求的 Context)

	// 缓存的状态，基于全局限制运行时不变动的假设。
	// 这些状态在 NewRateLimitedReader 中确定一次。
	globalLimitActiveAtCreation     bool // 创建时全局限速是否开启
	individualLimitActiveAtCreation bool // 创建时独立限速是否开启
	bypassLimiting                  bool // 创建时，如果全局和独立都无限速，则为 true
}

// NewRateLimitedReader 创建一个新的 RateLimitedReader。
// r: 底层的读取器 (如: resp.Body)。
// limit: 独立速率限制，单位是 Bytes/s (rate.Limit)。rate.Inf 表示无限制。
// burst: 独立令牌桶的突发容量，单位是字节。
// ctx: 与操作关联的 Context (如: 请求 Context)。
// 将 limit 设置为 <= 0 或 rate.Inf 将禁用此读取器的独立限速。
// 全局限制的激活状态是基于调用此函数时全局限制的状态确定的。
// !! 重要提示: 此 RateLimitedReader 的全局限速行为将固定为创建此实例时的全局状态。
func NewRateLimitedReader(r io.Reader, limit rate.Limit, burst int, ctx context.Context) *RateLimitedReader {
	// 确定独立限速器的激活状态
	individualLimiter := rate.NewLimiter(rate.Inf, 0) // 默认无限速
	individualLimitActive := false
	if limit > 0 && limit != rate.Inf {
		individualLimiter = rate.NewLimiter(limit, burst)
		individualLimitActive = true
	}

	// 确定创建 RateLimitedReader 实例时全局限速器的激活状态。
	// 使用 atomic.Load() 安全地获取当前的全局限速器。
	currentGlobalLimiterAtCreation := globalLimiter.Load()
	globalLimitActive := currentGlobalLimiterAtCreation.Limit() != rate.Inf

	// 确定是否可以完全绕过限速
	bypass := !globalLimitActive && !individualLimitActive

	return &RateLimitedReader{
		r:       r,
		limiter: individualLimiter,
		ctx:     ctx,

		globalLimitActiveAtCreation:     globalLimitActive,
		individualLimitActiveAtCreation: individualLimitActive,
		bypassLimiting:                  bypass,
	}
}

// Read 实现 io.Reader 接口。
// 在读取数据之前，根据缓存的状态决定是否需要向限速器申请许可。
func (rlr *RateLimitedReader) Read(p []byte) (n int, err error) {
	bytesToRequest := len(p)
	if bytesToRequest == 0 {
		// 请求读取 0 字节时，直接调用底层 Read 并立即返回
		return rlr.r.Read(p)
	}

	// 首先检查缓存的 bypassLimiting 状态。
	// 如果 bypassLimiting 为 true，意味着创建时全局和独立限速都无限速，
	// 且我们假设全局限速运行时不变，所以可以完全透穿。
	if rlr.bypassLimiting {
		return rlr.r.Read(p) // 完全跳过限速逻辑，直接透穿
	}

	// 如果执行到这里，说明创建时至少有一个限速器是激活的。
	// 我们根据创建时缓存的状态来决定是否需要等待。

	// 如果创建时全局限速是激活的，则应用全局限速。
	// 需要获取当前的全局限速器实例来调用 WaitN。
	// 在运行时不变动的假设下，globalLimiter.Load() 将始终返回同一个 rate.Limiter 实例的指针。
	if rlr.globalLimitActiveAtCreation {
		// 加载当前的全局限速器实例 (使用 atomic.Load)
		currentGlobalLimiter := globalLimiter.Load()
		// WaitN 会阻塞直到有令牌或 Context 被取消
		if err := currentGlobalLimiter.WaitN(rlr.ctx, bytesToRequest); err != nil {
			return 0, err
		}
	}

	// 如果创建时独立限速是激活的，则应用独立限速。
	if rlr.individualLimitActiveAtCreation {
		// rlr.limiter 是该 reader 的独立限速器实例，直接调用 WaitN
		if err := rlr.limiter.WaitN(rlr.ctx, bytesToRequest); err != nil {
			// WaitN 内部会检查 Context，即使在全局等待时 Context 已取消，这里也会正确处理
			return 0, err
		}
	}

	// 向底层的 Reader 读取数据
	n, err = rlr.r.Read(p)

	return n, err
}

// Close 实现 io.Closer 接口，转发 Close 调用给底层 Reader。
func (rlr *RateLimitedReader) Close() error {
	if closer, ok := rlr.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// 定义特殊error UnDefiendRateStringErr
type UnDefiendRateStringErr struct {
	s string
}

func (e *UnDefiendRateStringErr) Error() string {
	return e.s
}
