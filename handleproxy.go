// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.BSD file.
// Copyright 2025 Infinite-Iroha Group, WJQSERVER. All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.

// Touka reverse proxy handler

package toukautil

import (
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"

	"toukautil/ascii"

	"github.com/WJQSERVER-STUDIO/httpc"
	"github.com/infinite-iroha/touka" // 你的 Touka 框架包
	"golang.org/x/net/http/httpguts"
)

// --- 对象池 ---
var (
	// copyBufferPool 用于 io.CopyBuffer 操作，减少内存分配。
	copyBufferPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 32*1024) // 32KB 默认缓冲区大小
			return &b
		},
	}
)

// getCopyBuffer 从池中获取一个字节切片缓冲区。
func getCopyBuffer() *[]byte {
	return copyBufferPool.Get().(*[]byte)
}

// putCopyBuffer 将字节切片缓冲区返还到池中。
func putCopyBuffer(buf *[]byte) {
	copyBufferPool.Put(buf)
}

// --- 辅助函数 ---

// hopHeaders 定义了不应在代理之间转发的逐跳 HTTP 头部。
var hopHeaders = []string{
	"Connection",
	"Proxy-Connection", // 常见的非标准逐跳头部
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",      // 用于传输编码协商
	"Trailer", // 用于指示响应尾部存在的头部
	"Transfer-Encoding",
	"Upgrade", // 用于协议升级，如 WebSocket
}

// removeHopByHopHeaders 从给定的 http.Header 中移除所有逐跳头部。
// 这是实现透明代理的关键步骤。
func removeHopByHopHeaders(h http.Header) {
	// 根据 RFC 7230, section 6.1, 移除 "Connection" 头部中列出的所有字段
	for _, f := range h["Connection"] {
		for _, sf := range strings.Split(f, ",") {
			if sf = textproto.TrimString(sf); sf != "" {
				h.Del(sf)
			}
		}
	}
	// 移除已知的标准逐跳头部，以确保向后兼容性和覆盖所有情况
	for _, f := range hopHeaders {
		h.Del(f)
	}
}

// copyHeader 将所有头部从 src 复制到 dst。
// 对于多值头部，它会添加所有值。
func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// singleJoiningSlash 安全地拼接两个 URL 路径段。
// 它能处理尾部和头部的斜杠，确保结果只有一个斜杠分隔（如果需要）。
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash: // a/、/b -> a/b
		return a + b[1:]
	case !aslash && !bslash: // a、b -> a/b (除非 b 为空)
		if b == "" {
			return a
		}
		return a + "/" + b
	}
	// a/、b -> a/b  或  a、/b -> a/b
	return a + b
}

// upgradeType 从头部获取 "Upgrade" 字段的值，
// 前提是 "Connection" 头部包含 "Upgrade" token。
func upgradeType(h http.Header) string {
	if !httpguts.HeaderValuesContainsToken(h["Connection"], "Upgrade") {
		return ""
	}
	return h.Get("Upgrade")
}

// cloneURL 深拷贝一个 url.URL 对象，包括 Userinfo。
func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	u2 := new(url.URL) // 创建新实例
	*u2 = *u           // 复制值
	if u.User != nil { // 如果存在用户信息，也深拷贝
		u2.User = new(url.Userinfo)
		*u2.User = *u.User
	}
	return u2
}

// isNetErrClosed 检查错误是否表示网络连接已关闭的常见错误类型。
func isNetErrClosed(err error) bool {
	if err == nil {
		return false
	}
	// 检查常见的网络关闭错误字符串或错误类型
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "broken pipe") ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) // io.EOF 在 io.Copy 中通常表示连接正常关闭
}

// ResponseMiddleware 是一个在后端响应头已写入客户端，但在响应体复制之前执行的中间件类型。
// 它允许在最后时刻修改客户端响应（主要是头部）或执行其他操作。
// 如果中间件返回错误，将中止响应体的复制。
type ResponseMiddleware func(backendResp *http.Response, clientResWriter http.ResponseWriter) error

// ReverseProxyConfig 包含反向代理的配置选项。
type ReverseProxyConfig struct {
	// TargetURL 是后端目标服务器的基础 URL 字符串。
	// 例如 "http://localhost:8081/api" 或 "https://service.example.com"。
	// 对于 HandleReverseProxy，此字段会被使用。
	// 对于 SingleReverseProxy，此字段会被参数 targetURL 覆盖。
	TargetURL string

	// HTTPClient 是用于执行出站请求的 httpc.Client 实例。
	// 如果为 nil，将尝试从 touka.Context 中获取。
	HTTPClient *httpc.Client

	// RewriteRequest 是一个可选的回调函数，在发送请求到后端之前修改出站请求。
	// 参数 inReq 是原始的入站请求 (来自客户端)。
	// 参数 outReqBuilder 是 httpc.RequestBuilder，用于构建出站请求。
	// 如果此函数返回错误，代理将中止并调用 ErrorHandler。
	RewriteRequest func(inReq *http.Request, outReqBuilder *httpc.RequestBuilder) error

	// ModifyResponse 是一个可选的回调函数，在收到后端响应后、
	// 将其头部和状态码应用到客户端响应之前调用。
	// 允许检查或修改将要写回客户端的响应（主要是头部）。
	// 如果此函数返回错误，代理将调用 ErrorHandler。
	ModifyResponse func(backendResp *http.Response) error

	// ResponseMiddlewares 是一个响应处理中间件切片。
	// 它们在后端响应头和状态码已写入客户端之后、响应体开始复制之前按顺序执行。
	ResponseMiddlewares []ResponseMiddleware

	// ErrorHandler 是一个可选函数，用于处理代理过程中发生的错误。
	// 如果为 nil，将使用默认的错误处理。
	ErrorHandler func(c *touka.Context, err error)

	// FlushInterval 指定在复制响应体时刷新到客户端的刷新间隔。
	// 零表示不周期性刷新。负值表示每次写入后立即刷新。
	// 对于流式响应 (ContentLength -1 或 Content-Type text/event-stream) 会被覆盖为立即刷新。
	FlushInterval time.Duration
}

// defaultProxyErrorHandler 是默认的错误处理器。
// 它记录错误并通过 Touka Context 返回一个 502 Bad Gateway 响应。
func defaultProxyErrorHandler(c *touka.Context, err error) {
	// 使用英文记录日志
	log.Printf("toukautil.ReverseProxy: Error for request %s %s: %v", c.Request.Method, c.Request.URL.Path, err)
	if !c.Writer.Written() { // 只有在响应头还没写入时才发送错误响应
		// 返回英文错误
		c.String(http.StatusBadGateway, "Proxy Error: %s", err.Error())
	}
	if !c.IsAborted() { // 确保 Touka 的处理链被中止
		c.Abort()
	}
}

// prepareReverseProxy validates config and sets up necessary components.
// It returns the target URL, http client, and error handler to use.
// 这是一个内部辅助函数，用于初始化和验证代理配置。
func prepareReverseProxy(c *touka.Context, config *ReverseProxyConfig, dynamicTargetURL ...string) (target *url.URL, httpClient *httpc.Client, errorHandler func(*touka.Context, error), err error) {
	// 确定目标 URL
	targetURLStr := config.TargetURL
	if len(dynamicTargetURL) > 0 && dynamicTargetURL[0] != "" {
		targetURLStr = dynamicTargetURL[0]
	}
	if targetURLStr == "" {
		err = errors.New("TargetURL is not configured and not provided dynamically") // 英文错误
		return
	}

	target, err = url.Parse(targetURLStr)
	if err != nil {
		err = fmt.Errorf("failed to parse TargetURL '%s': %w", targetURLStr, err) // 英文错误
		return
	}
	if target.Scheme == "" || target.Host == "" {
		err = fmt.Errorf("TargetURL '%s' must have a scheme and host", targetURLStr) // 英文错误
		return
	}

	// 获取 HTTPClient
	httpClient = config.HTTPClient
	if httpClient == nil {
		httpClient = c.GetHTTPC()
		if httpClient == nil {
			err = errors.New("HTTPClient is not available from Touka Context and not provided in ReverseProxyConfig") // 英文错误
			return
		}
	}

	// 获取 ErrorHandler
	errorHandler = config.ErrorHandler
	if errorHandler == nil {
		errorHandler = defaultProxyErrorHandler
	}
	return
}

// ServeReverseProxyCore 是反向代理的核心逻辑。
// 它构建并发送请求到后端，处理 1xx 和协议升级，并将后端响应的头部和状态码写入客户端。
// 它返回后端响应对象（用于后续处理响应体）和可能发生的错误。
// 调用者负责关闭返回的 backendResp.Body。
func ServeReverseProxyCore(c *touka.Context, config ReverseProxyConfig, target *url.URL, httpClient *httpc.Client, errorHandler func(*touka.Context, error)) (backendResp *http.Response, err error) {
	inReq := c.Request

	// 1. 使用 httpc.RequestBuilder 准备出站请求
	finalTargetPath := singleJoiningSlash(target.Path, inReq.URL.Path)
	finalTargetQuery := target.RawQuery
	inReqQuery := inReq.URL.RawQuery
	if finalTargetQuery == "" || inReqQuery == "" {
		finalTargetQuery = finalTargetQuery + inReqQuery
	} else {
		finalTargetQuery = finalTargetQuery + "&" + inReqQuery
	}
	var finalRawPath string
	if target.RawPath != "" || inReq.URL.RawPath != "" {
		finalRawPath = singleJoiningSlash(target.EscapedPath(), inReq.URL.EscapedPath())
	}
	outURL := &url.URL{
		Scheme:   target.Scheme,
		Host:     target.Host,
		Path:     finalTargetPath,
		RawPath:  finalRawPath,
		RawQuery: finalTargetQuery,
	}

	outReqBuilder := httpClient.NewRequestBuilder(inReq.Method, outURL.String())
	outReqBuilder.WithContext(inReq.Context()) // 传递上下文，用于超时和取消
	outReqBuilder.NoDefaultHeaders()           // 通常代理不应添加默认头部，而是转发或按需修改

	// 准备要转发的头部
	tempOutHeader := make(http.Header)
	copyHeader(tempOutHeader, inReq.Header) // 从入站请求复制所有头部

	outReqOriginalUpgrade := upgradeType(tempOutHeader) // 检查是否有 Upgrade 意图
	removeHopByHopHeaders(tempOutHeader)                // 从临时头部中移除逐跳头部
	if outReqOriginalUpgrade != "" {                    // 如果是升级请求
		if !ascii.IsPrint(outReqOriginalUpgrade) { // 验证 Upgrade 协议名
			return nil, fmt.Errorf("client tried to switch to invalid protocol %q", outReqOriginalUpgrade) // 英文错误
		}
		// 重新设置标准的 Upgrade 头部
		tempOutHeader.Set("Connection", "Upgrade")
		tempOutHeader.Set("Upgrade", outReqOriginalUpgrade)
	}
	// 如果客户端明确要求 trailers，则也告知后端
	if httpguts.HeaderValuesContainsToken(inReq.Header["Te"], "trailers") {
		tempOutHeader.Set("Te", "trailers")
	}
	// 如果原始请求没有 User-Agent，则不发送 Go 的默认 User-Agent
	if _, ok := inReq.Header["User-Agent"]; !ok {
		tempOutHeader.Set("User-Agent", "")
	}
	// 将处理过的头部添加到 RequestBuilder
	for k, vv := range tempOutHeader {
		for _, v := range vv {
			outReqBuilder.AddHeader(k, v)
		}
	}

	// 设置请求体
	if inReq.Body != nil && inReq.Body != http.NoBody {
		outReqBuilder.SetBody(inReq.Body)
		// ContentLength 的处理: httpc.RequestBuilder 在 Build 时会尝试从 Body 推断。
		// 如果原始请求有 ContentLength，并且 Body 是可重读的 (有 GetBody)，
		// RewriteRequest 中可能需要确保最终的 *http.Request 有正确的 ContentLength。
	}

	// 调用用户提供的 RewriteRequest 回调
	if config.RewriteRequest != nil {
		if errRewrite := config.RewriteRequest(inReq, outReqBuilder); errRewrite != nil {
			return nil, fmt.Errorf("RewriteRequest failed: %w", errRewrite) // 英文错误
		}
	}

	// 构建最终的出站请求
	finalOutReq, buildErr := outReqBuilder.Build()
	if buildErr != nil {
		return nil, fmt.Errorf("failed to build outgoing request: %w", buildErr) // 英文错误
	}

	// 2. 发送请求到后端，并处理 1xx 响应
	var roundTripErr error
	var roundTripMutex sync.Mutex
	var roundTripDone bool

	trace := &httptrace.ClientTrace{
		Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
			roundTripMutex.Lock()
			defer roundTripMutex.Unlock()
			if roundTripDone { // 如果主 RoundTrip 已完成，则忽略1xx
				return nil
			}
			// 将 1xx 响应头和状态码写回客户端
			clientResWriterHeaders := c.Writer.Header()
			copyHeader(clientResWriterHeaders, http.Header(header))
			c.Writer.WriteHeader(code)
			// 清理头部，因为 WriteHeader 对 1xx 不会自动清除
			for k := range clientResWriterHeaders {
				delete(clientResWriterHeaders, k)
			}
			return nil
		},
	}
	finalOutReq = finalOutReq.WithContext(httptrace.WithClientTrace(finalOutReq.Context(), trace))

	// 使用 httpc.Client 执行请求
	backendResp, roundTripErr = httpClient.Do(finalOutReq)
	roundTripMutex.Lock()
	roundTripDone = true // 标记 RoundTrip 完成，后续的 Got1xxResponse 应被忽略
	roundTripMutex.Unlock()

	if roundTripErr != nil {
		// 返回错误，让上层调用 errorHandler
		return nil, fmt.Errorf("request to backend failed via httpc: %w", roundTripErr) // 英文错误
	}
	// 注意：backendResp.Body 此时尚未关闭，由本函数的调用者负责。

	// 3. 初步处理后端响应
	// 如果是协议切换 (101)，特殊处理
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// handleProtocolUpgrade 会负责写入 101 响应头并劫持连接
		if errUpgrade := handleProtocolUpgrade(c, inReq, backendResp); errUpgrade != nil {
			// errorHandler(c, errUpgrade) // 不在这里调用 errorHandler，而是返回错误
			return backendResp, errUpgrade // 返回响应体（被劫持的连接）和错误
		}
		// 升级成功，backendResp.Body 实际上是被劫持的连接，ServeReverseProxy 不再复制它
		return backendResp, nil // 返回 nil 错误表示升级成功，由调用者判断是否继续
	}

	// 对于非 101 响应，移除逐跳头部
	removeHopByHopHeaders(backendResp.Header)

	// 调用用户提供的 ModifyResponse 回调
	if config.ModifyResponse != nil {
		if errModify := config.ModifyResponse(backendResp); errModify != nil {
			// errorHandler(c, fmt.Errorf("ModifyResponse failed: %w", errModify))
			return backendResp, fmt.Errorf("ModifyResponse failed: %w", errModify) // 返回响应体和错误
		}
	}
	return backendResp, nil // 返回后端响应和 nil 错误
}

// ServeReverseProxy 对给定的 Touka Context 执行完整的反向代理操作。
// 包括发送请求、处理响应头、应用响应中间件、复制响应体和处理 Trailer。
func ServeReverseProxy(c *touka.Context, config ReverseProxyConfig, dynamicTargetURL ...string) {
	target, httpClient, errorHandler, err := prepareReverseProxy(c, &config, dynamicTargetURL...)
	if err != nil {
		// prepareReverseProxy 内部没有 errorHandler, 所以这里需要调用
		// 如果 config.ErrorHandler 为 nil, prepare 会使用 defaultProxyErrorHandler,
		// 但它需要一个 *touka.Context, 我们在 prepare 阶段还没有。
		// 所以 prepare 应该只返回错误，让调用它的地方（这里）处理。
		if config.ErrorHandler == nil { // 如果用户没配，我们用默认的
			defaultProxyErrorHandler(c, err)
		} else {
			config.ErrorHandler(c, err)
		}
		return
	}

	// 调用核心代理逻辑获取后端响应（头部已处理，Body 待处理）
	backendResp, coreErr := ServeReverseProxyCore(c, config, target, httpClient, errorHandler)
	if coreErr != nil {
		// 如果 backendResp 不为 nil，表示后端有响应但处理失败，需要关闭 Body
		if backendResp != nil && backendResp.Body != nil {
			io.Copy(io.Discard, backendResp.Body) // 尝试消耗 Body
			backendResp.Body.Close()
		}
		errorHandler(c, coreErr)
		return
	}
	// ServeReverseProxyCore 成功后，backendResp.Body 仍需关闭
	defer func() {
		if backendResp != nil && backendResp.Body != nil {
			io.Copy(io.Discard, backendResp.Body)
			backendResp.Body.Close()
		}
	}()

	// 如果是协议升级，核心逻辑已经处理完毕并返回
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// 此时连接已劫持，不需要进一步处理HTTP响应体
		return
	}

	// 将后端响应头和状态码复制到客户端 ResponseWriter
	copyHeader(c.Writer.Header(), backendResp.Header)
	// 处理响应 Trailer 声明
	announcedTrailers := len(backendResp.Trailer)
	if announcedTrailers > 0 {
		trailerKeys := make([]string, 0, len(backendResp.Trailer))
		for k := range backendResp.Trailer {
			trailerKeys = append(trailerKeys, k)
		}
		c.Writer.Header().Add("Trailer", strings.Join(trailerKeys, ", "))
	}
	c.Writer.WriteHeader(backendResp.StatusCode) // 写入状态码

	// 执行响应处理中间件
	if len(config.ResponseMiddlewares) > 0 {
		for _, mw := range config.ResponseMiddlewares {
			if err := mw(backendResp, c.Writer); err != nil {
				// 中间件返回错误，记录并可能中止（取决于中间件实现）
				// 此处我们选择调用 errorHandler，它应该负责中止
				errorHandler(c, fmt.Errorf("ResponseMiddleware failed: %w", err)) // 英文错误
				return                                                            // 中止后续中间件和响应体复制
			}
		}
	}

	// 复制响应体到客户端
	flushInterval := determineActualFlushInterval(backendResp, config.FlushInterval)
	if err := copyProxiedResponseBody(c.Writer, backendResp.Body, flushInterval); err != nil {
		// 记录复制响应体时的错误，此时头部和状态码已发送
		// 通常是客户端断开连接
		log.Printf("toukautil.ServeReverseProxy: Error copying response body for %s: %v", c.Request.URL.Path, err) // 英文日志
	}

	// 复制实际的 Trailer 头部（如果存在且与声明匹配）
	if len(backendResp.Trailer) > 0 && len(backendResp.Trailer) == announcedTrailers {
		copyHeader(c.Writer.Header(), backendResp.Trailer)
	} else if len(backendResp.Trailer) > 0 { // 如果后端发送了未声明的 Trailer，按 TrailerPrefix 添加
		for k, vv := range backendResp.Trailer {
			k = http.TrailerPrefix + k
			for _, v := range vv {
				c.Writer.Header().Add(k, v)
			}
		}
	}
	// 代理操作完成
}

// HandleReverseProxy 实现类似 Touka Handler 的反向代理。
// 它使用 config 中定义的 TargetURL。
// 这个函数提供了一个便捷的方式在 Touka 路由中直接使用反向代理。
func HandleReverseProxy(c *touka.Context, config ReverseProxyConfig) {
	ServeReverseProxy(c, config) // 直接调用 ServeReverseProxy
}

// SingleReverseProxy 将当前请求反向代理到指定的 targetURL。
// 它会合并传入的 config，但 targetURL 参数会覆盖 config.TargetURL。
// 这个函数适用于需要根据运行时信息动态决定代理目标的场景。
func SingleReverseProxy(targetURL string, c *touka.Context, config ReverseProxyConfig) {
	// dynamicTargetURL 可选参数用于覆盖 config.TargetURL
	ServeReverseProxy(c, config, targetURL)
}

// (determineActualFlushInterval, copyProxiedResponseBody 保持不变)
func determineActualFlushInterval(res *http.Response, configuredInterval time.Duration) time.Duration {
	resCT := res.Header.Get("Content-Type")
	if baseCT, _, _ := mime.ParseMediaType(resCT); baseCT == "text/event-stream" {
		return -1
	}
	if res.ContentLength == -1 {
		return -1
	}
	return configuredInterval
}

func copyProxiedResponseBody(dst http.ResponseWriter, src io.Reader, flushInterval time.Duration) error {
	var w io.Writer = dst
	var flusher http.Flusher
	if f, ok := dst.(http.Flusher); ok {
		flusher = f
	}
	if flushInterval != 0 && flusher != nil {
		mlw := newMaxLatencyWriter(dst, flushInterval, flusher)
		defer mlw.stop()
		w = mlw
	}
	bufPtr := getCopyBuffer()
	defer putCopyBuffer(bufPtr)
	buf := *bufPtr
	_, err := io.CopyBuffer(w, src, buf)
	return err
}

// handleProtocolUpgrade 处理协议升级响应 (如 WebSocket)。
// (与你提供的最新版本基本一致，确保 ascii.IsPrint 和 ascii.EqualFold 可用)
func handleProtocolUpgrade(c *touka.Context, inReq *http.Request, backendResp *http.Response) error {
	clientResWriter := c.Writer
	inReqUpgradeType := upgradeType(inReq.Header)
	backendRespUpgradeType := upgradeType(backendResp.Header)

	if !ascii.IsPrint(backendRespUpgradeType) {
		return fmt.Errorf("backend tried to switch to invalid protocol: %q", backendRespUpgradeType)
	}
	if !ascii.EqualFold(inReqUpgradeType, backendRespUpgradeType) { // 使用 ascii.EqualFold
		return fmt.Errorf("backend tried to switch protocol %q when %q was requested", backendRespUpgradeType, inReqUpgradeType)
	}

	backendConn, ok := backendResp.Body.(io.ReadWriteCloser)
	if !ok {
		return errors.New("internal error: 101 Switching Protocols response with non-writable body")
	}
	// 注意：backendConn (即 backendResp.Body) 的关闭由 ServeReverseProxyCore 的调用者 (ServeReverseProxy) 的 defer 处理

	hijacker, ok := clientResWriter.(http.Hijacker)
	if !ok {
		return errors.New("client ResponseWriter does not support Hijack for protocol switch")
	}
	clientConn, clientBrw, err := hijacker.Hijack() // clientBrw 是 *bufio.ReadWriter
	if err != nil {
		return fmt.Errorf("failed to hijack client connection: %w", err)
	}
	defer clientConn.Close() // 确保客户端劫持的连接最终被关闭

	// 监听上下文取消，如果取消则尝试关闭后端连接
	// 这是为了在双向复制过程中，如果原始请求的上下文被取消，能尽早中断与后端的连接
	ctxDone := inReq.Context().Done()
	if ctxDone != nil {
		// 创建一个 channel 来信号通知 goroutine 停止，防止 goroutine泄漏
		stopBackendWatcher := make(chan struct{})
		defer close(stopBackendWatcher) // 确保 watcher goroutine 会退出

		go func() {
			select {
			case <-ctxDone:
				log.Printf("toukautil.handleProtocolUpgrade: Context cancelled, closing backend connection for %s", inReq.URL.Path) // 英文日志
				backendConn.Close()
			case <-stopBackendWatcher: // 如果 handleProtocolUpgrade 正常返回，也停止这个 watcher
				return
			}
		}()
	}

	// 将 101 响应（包括头部）写入劫持的客户端连接
	if backendResp.ProtoMajor == 0 { // 确保协议版本被设置
		backendResp.ProtoMajor = 1
		backendResp.ProtoMinor = 1
	}
	// Write 会写入 StatusLine 和 Header，然后是 Body（如果 Body 非 nil 且非 NoBody）
	// 由于我们是 101，Body 应该是 nil，因为实际数据通过劫持的连接传输
	// 为了确保只写头部，可以临时将 Body 设为 nil，然后再恢复（如果需要）
	// 但 backendResp.Body 已经是 backendConn，所以 Write 不会写 Body。
	if err = backendResp.Write(clientBrw); err != nil {
		return fmt.Errorf("error writing 101 response to client: %w", err) // 英文错误
	}
	if err = clientBrw.Flush(); err != nil { // 确保头部已发送
		return fmt.Errorf("error flushing 101 response to client: %w", err) // 英文错误
	}

	// 启动双向复制数据
	errChan := make(chan error, 1) // 缓冲为1，只关心第一个发生的错误
	var wg sync.WaitGroup
	wg.Add(2) // 等待两个复制 goroutine 完成

	go func() {
		defer wg.Done()
		_, copyErr := io.Copy(clientConn, backendConn) // 后端 -> 客户端
		if copyErr != nil && !isNetErrClosed(copyErr) {
			// 只有在不是预期的关闭错误时才发送到 errChan
			select {
			case errChan <- fmt.Errorf("copy from backend to client: %w", copyErr): // 英文错误
			default:
			}
		}
		clientConn.Close() // 主动关闭一端以尝试通知另一端
	}()

	go func() {
		defer wg.Done()
		_, copyErr := io.Copy(backendConn, clientConn) // 客户端 -> 后端
		if copyErr != nil && !isNetErrClosed(copyErr) {
			select {
			case errChan <- fmt.Errorf("copy from client to backend: %w", copyErr): // 英文错误
			default:
			}
		}
		backendConn.Close() // 主动关闭一端以尝试通知另一端
	}()

	// 等待所有复制完成或第一个错误发生或上下文取消
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case err = <-errChan: // 捕获第一个复制错误
		if err != nil { // 仅当确实是错误时（非nil）
			log.Printf("toukautil.handleProtocolUpgrade: Data copy error: %v", err) // 英文日志
			return err
		}
		// 如果 err 是 nil (例如，一个 copy goroutine 正常结束发送 nil 到 errChan)
		// 我们需要等待另一个 copy 也完成，或者上下文取消
		select {
		case <-waitDone: // 另一个也完成了
			return nil
		case <-inReq.Context().Done():
			log.Printf("toukautil.handleProtocolUpgrade: Context cancelled during upgrade for %s", inReq.URL.Path) // 英文日志
			return fmt.Errorf("context cancelled during upgrade: %w", inReq.Context().Err())                       // 英文错误
		}
	case <-waitDone: // 所有复制正常完成
		return nil
	case <-inReq.Context().Done(): // 上下文在任何复制错误或完成之前取消
		log.Printf("toukautil.handleProtocolUpgrade: Context cancelled during upgrade for %s", inReq.URL.Path) // 英文日志
		return fmt.Errorf("context cancelled during upgrade: %w", inReq.Context().Err())
	}
}

// maxLatencyWriter (与你提供的最新版本一致)
type maxLatencyWriter struct {
	dst          io.Writer
	flusher      http.Flusher
	latency      time.Duration
	mu           sync.Mutex
	timer        *time.Timer
	flushPending bool
}

func newMaxLatencyWriter(dst io.Writer, latency time.Duration, flusher http.Flusher) *maxLatencyWriter {
	mlw := &maxLatencyWriter{
		dst:     dst,
		latency: latency,
		flusher: flusher,
	}
	// 只有当 latency > 0 且 flusher 有效时，才启动定时器
	// 并且，初始写入时 Write 方法会负责启动或重置定时器
	return mlw
}

func (m *maxLatencyWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	n, err = m.dst.Write(p) // 先执行写入
	m.mu.Unlock()           // 提前解锁，允许并发写入（如果 dst 支持）

	if m.flusher == nil { // 如果没有 flusher，直接返回写入结果
		return
	}

	m.mu.Lock() // 再次加锁以保护 timer 和 flushPending
	defer m.mu.Unlock()

	if m.latency < 0 { // 负延迟表示立即刷新
		m.flusher.Flush()
		return
	}

	// 对于正延迟
	m.flushPending = true // 标记有数据等待刷新
	if m.timer == nil {   // 如果计时器未启动
		m.timer = time.AfterFunc(m.latency, m.delayedFlush)
	} else {
		m.timer.Reset(m.latency) // 重置计时器
	}
	return
}

func (m *maxLatencyWriter) delayedFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.flushPending || m.flusher == nil { // 如果没有待刷新数据或没有 flusher
		return
	}
	m.flusher.Flush() // 执行刷新，忽略错误（因为是后台任务，此时可能无法有效报告错误）
	m.flushPending = false
}

func (m *maxLatencyWriter) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false // 停止时，取消待刷新标记
	if m.timer != nil {
		m.timer.Stop() // 停止计时器
	}
}
