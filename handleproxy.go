// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.BSD file.
// Copyright 2025 Infinite-Iroha Group, WJQSERVER. All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.

// Touka reverse proxy handler

// 文件: toukautil/reverse_proxy.go
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
	// 如果此函数返回错误，代理将中止并调用 ErrorHandler (在自动模式下) 或返回错误 (在手动模式下)。
	RewriteRequest func(inReq *http.Request, outReqBuilder *httpc.RequestBuilder) error

	// ModifyResponse 是一个可选的回调函数，在收到后端响应后、
	// 将其头部和状态码应用到客户端响应之前调用 (在自动模式下)
	// 或在返回给手动模式调用者之前调用。
	// 允许检查或修改将要写回客户端的响应（主要是头部）。
	// 如果此函数返回错误，代理将调用 ErrorHandler (在自动模式下) 或返回错误 (在手动模式下)。
	ModifyResponse func(backendResp *http.Response) error

	// ResponseMiddlewares 是一个响应处理中间件切片。
	// 它们在后端响应头和状态码已写入客户端之后、响应体开始复制之前按顺序执行。
	// 注意: 此选项仅在 ServeReverseProxy (自动模式) 中生效。
	ResponseMiddlewares []ResponseMiddleware

	// ErrorHandler 是一个可选函数，用于处理代理过程中发生的错误。
	// 如果为 nil，将使用默认的错误处理。
	// 注意: 此处理器主要由 ServeReverseProxy (自动模式) 调用。
	// 手动模式函数 (如 ServeReverseProxyManual) 会直接返回错误。
	ErrorHandler func(c *touka.Context, err error)

	// FlushInterval 指定在复制响应体时刷新到客户端的刷新间隔。
	// 零表示不周期性刷新。负值表示每次写入后立即刷新。
	// 对于流式响应 (ContentLength -1 或 Content-Type text/event-stream) 会被覆盖为立即刷新。
	// 注意: 此选项仅在 ServeReverseProxy (自动模式) 中生效。
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
			// 注意: 在手动模式下，用户需要自己处理1xx，这里写入 Touka Context 的 Writer 可能不适用
			// 但 ServeReverseProxyCore 是通用核心，ServeReverseProxy 会用这个。
			// 对于 ServeReverseProxyManual，用户不会直接看到这里的1xx写入，他们会得到最终的 backendResp。
			// 如果需要手动处理1xx，需要更复杂的接口。目前保持现状。
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
		// 返回错误，让上层调用 errorHandler (在自动模式下) 或直接返回 (在手动模式下)
		// backendResp 可能是 nil，也可能包含部分响应（例如连接错误但有响应头）
		return backendResp, fmt.Errorf("request to backend failed via httpc: %w", roundTripErr) // 英文错误
	}
	// 注意：backendResp.Body 此时尚未关闭，由本函数的调用者负责。

	// 3. 初步处理后端响应
	// 如果是协议切换 (101)，特殊处理
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// handleProtocolUpgrade 会负责写入 101 响应头并劫持连接 (对于自动模式)
		// 对于手动模式，它主要设置好劫持，具体的写入由用户决定。
		// ServeReverseProxyCore 的调用者需要知道这是个升级响应。
		if errUpgrade := handleProtocolUpgrade(c, inReq, backendResp); errUpgrade != nil {
			return backendResp, errUpgrade // 返回响应体（被劫持的连接）和错误
		}
		// 升级成功，backendResp.Body 实际上是被劫持的连接。
		return backendResp, nil // 返回 nil 错误表示升级成功
	}

	// 对于非 101 响应，移除逐跳头部
	removeHopByHopHeaders(backendResp.Header)

	// 调用用户提供的 ModifyResponse 回调
	if config.ModifyResponse != nil {
		if errModify := config.ModifyResponse(backendResp); errModify != nil {
			return backendResp, fmt.Errorf("ModifyResponse failed: %w", errModify) // 返回响应体和错误
		}
	}
	return backendResp, nil // 返回后端响应和 nil 错误
}

// ServeReverseProxyManual 允许用户手动处理反向代理的响应。
// 它执行代理请求的核心逻辑，处理协议升级，并返回处理后的后端响应头和整个后端响应。
//
// 返回值:
//   - processedBackendHeader (http.Header): 处理后的后端响应头。
//     在成功时，这是 backendResp.Header (已移除逐跳头部并应用 ModifyResponse)。
//     如果 backendResp 为 nil (例如 prepareReverseProxy 失败，或 RewriteRequest 失败)，则此为 nil。
//     如果 ServeReverseProxyCore 返回错误但 backendResp 非 nil，则此为 backendResp.Header。
//   - backendResp (*http.Response): 从后端获取的原始 *http.Response。
//     调用者【始终负责】关闭 backendResp.Body (如果 backendResp 非 nil)，即使在发生错误时也是如此。
//   - err (error): 代理过程中发生的任何错误。
//
// 调用者责任:
// 1. 检查返回的 err。
// 2. 如果 backendResp 非 nil，必须调用 backendResp.Body.Close() 来释放资源。
// 3. 自行处理 processedBackendHeader 和 backendResp，例如将响应写回客户端。
//
// 协议升级:
// 如果 err 为 nil 且 backendResp.StatusCode 是 http.StatusSwitchingProtocols (101)，
// 则表示协议升级已准备就绪。backendResp.Body 将是一个 io.ReadWriteCloser (通常是底层的 net.Conn)，
// 代表与后端服务器的劫持连接。调用者需要负责与此连接进行双向通信，并最终关闭它。
// `handleProtocolUpgrade` 内部会尝试向客户端写入101头部，但对于手动模式，
// 用户可能需要更细致地控制这个过程，或者接受这种默认行为。
func ServeReverseProxyManual(
	c *touka.Context,
	config ReverseProxyConfig,
	dynamicTargetURL ...string,
) (processedBackendHeader http.Header, backendResp *http.Response, err error) {
	target, httpClient, errorHandler, prepErr := prepareReverseProxy(c, &config, dynamicTargetURL...)
	if prepErr != nil {
		// prepareReverseProxy 失败，直接返回错误
		return nil, nil, fmt.Errorf("failed to prepare reverse proxy: %w", prepErr) // 英文错误
	}

	// 调用核心代理逻辑获取后端响应。
	// 从 ServeReverseProxyCore 返回的 backendResp，其 Body 的关闭责任传递给本函数的调用者。
	// errorHandler 参数主要供 ServeReverseProxyCore 内部（如果需要）或其更上层（如 ServeReverseProxy）使用。
	// ServeReverseProxyManual 自身不调用 errorHandler，而是返回错误。
	coreResp, coreErr := ServeReverseProxyCore(c, config, target, httpClient, errorHandler)

	if coreErr != nil {
		// 核心逻辑出错。将 coreResp (可能为 nil 或部分有效) 和 coreErr 一起返回。
		// 调用者需要检查 coreErr，并且如果 coreResp 非 nil，仍需关闭其 Body。
		var hdr http.Header
		if coreResp != nil {
			hdr = coreResp.Header // 即使有错，如果响应对象存在，头部也可能存在
		}
		// 返回原始的 coreResp，让调用者决定如何处理，包括关闭其 Body
		return hdr, coreResp, fmt.Errorf("ServeReverseProxyCore error: %w", coreErr) // 英文错误
	}

	// 此时 coreResp 是有效的后端响应 (或协议升级后的响应)
	// coreResp.Header 已经是处理过的头部 (移除了 hop-by-hop，应用了 ModifyResponse)
	return coreResp.Header, coreResp, nil
}

// ServeReverseProxy 对给定的 Touka Context 执行完整的反向代理操作。
// 包括发送请求、处理响应头、应用响应中间件、复制响应体和处理 Trailer。
// 这是一个“自动模式”的代理。
func ServeReverseProxy(c *touka.Context, config ReverseProxyConfig, dynamicTargetURL ...string) {
	target, httpClient, errorHandler, err := prepareReverseProxy(c, &config, dynamicTargetURL...)
	if err != nil {
		// 如果 prepareReverseProxy 失败，则调用 errorHandler。
		// errorHandler 来自 config 或者 defaultProxyErrorHandler。
		errorHandler(c, fmt.Errorf("failed to prepare reverse proxy for ServeReverseProxy: %w", err)) // 英文错误
		return
	}

	// 调用核心代理逻辑获取后端响应（头部已处理，Body 待处理）
	backendResp, coreErr := ServeReverseProxyCore(c, config, target, httpClient, errorHandler)
	// ServeReverseProxyCore 的调用者（即本函数）负责关闭 backendResp.Body。
	// 我们使用 defer 来确保在各种情况下（成功、失败、panic）都能关闭。
	if backendResp != nil && backendResp.Body != nil {
		defer func() {
			io.Copy(io.Discard, backendResp.Body) // 尝试消耗剩余的 Body 内容
			backendResp.Body.Close()
		}()
	}

	if coreErr != nil {
		// 核心代理逻辑发生错误。此时 backendResp.Body 已经通过 defer 安排关闭。
		errorHandler(c, fmt.Errorf("ServeReverseProxyCore failed in ServeReverseProxy: %w", coreErr)) // 英文错误
		return
	}

	// 如果是协议升级，核心逻辑已经处理完毕并返回 (backendResp.StatusCode == 101)
	// ServeReverseProxyCore 中的 handleProtocolUpgrade 已经处理了与客户端的交互。
	// backendResp.Body (即劫持的连接) 的关闭由 handleProtocolUpgrade 内部或其 defer 栈管理。
	// 对于 ServeReverseProxy，在协议升级成功后，主要工作已完成。
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// 连接已劫持，不需要进一步处理HTTP响应体。
		// backendResp.Body (劫持的连接) 的生命周期由 handleProtocolUpgrade 管理。
		// 此处的 defer close (上面针对 backendResp.Body 的) 在这种情况下，
		// 如果 backendResp.Body 被替换为劫持连接，原始的 Body 可能已经关闭或不存在。
		// handleProtocolUpgrade 返回的 backendResp.Body 是劫持连接，它的关闭由 handleProtocolUpgrade 中的逻辑（如双向复制完成或错误后）处理。
		return
	}

	// --- 对于非协议升级的成功响应 ---

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
			// 注意：中间件操作的是 backendResp 和 c.Writer
			// backendResp.Body 此时还未开始复制
			if errMw := mw(backendResp, c.Writer); errMw != nil {
				errorHandler(c, fmt.Errorf("ResponseMiddleware failed: %w", errMw)) // 英文错误
				// 中间件出错，此时头部可能已发送，响应体未发送。
				// errorHandler 应该负责中止。
				return
			}
		}
	}

	// 复制响应体到客户端
	// backendResp.Body 将在 defer 中关闭
	flushInterval := determineActualFlushInterval(backendResp, config.FlushInterval)
	if copyErr := copyProxiedResponseBody(c.Writer, backendResp.Body, flushInterval); copyErr != nil {
		// 记录复制响应体时的错误，此时头部和状态码已发送
		// 通常是客户端断开连接或上下文取消
		if !isNetErrClosed(copyErr) && !errors.Is(copyErr, c.Request.Context().Err()) {
			log.Printf("toukautil.ServeReverseProxy: Error copying response body for %s: %v", c.Request.URL.Path, copyErr) // 英文日志
		}
		// 即使复制出错，也让 defer 去关闭 backendResp.Body
		// errorHandler 在这里不一定适合调用，因为响应头已发送。
		// Touka 的 context 可能会因客户端断开而自动中止。
		return
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

// determineActualFlushInterval 根据响应类型和配置确定实际的刷新间隔。
func determineActualFlushInterval(res *http.Response, configuredInterval time.Duration) time.Duration {
	resCT := res.Header.Get("Content-Type")
	// 解析媒体类型，忽略参数（如 charset）
	if baseCT, _, err := mime.ParseMediaType(resCT); err == nil && baseCT == "text/event-stream" {
		return -1 // 对于 Server-Sent Events，立即刷新
	}
	// 如果 ContentLength 未知（例如流式传输），也倾向于立即刷新
	if res.ContentLength == -1 {
		return -1
	}
	// 否则使用配置的间隔
	return configuredInterval
}

// copyProxiedResponseBody 将响应体从 src 复制到 dst，支持周期性刷新。
func copyProxiedResponseBody(dst http.ResponseWriter, src io.Reader, flushInterval time.Duration) error {
	var w io.Writer = dst
	var flusher http.Flusher
	if f, ok := dst.(http.Flusher); ok {
		flusher = f
	}

	// 如果需要刷新且 ResponseWriter 支持 Flush
	if flushInterval != 0 && flusher != nil {
		mlw := newMaxLatencyWriter(dst, flushInterval, flusher)
		defer mlw.stop() // 确保 maxLatencyWriter 的资源被释放
		w = mlw
	}

	bufPtr := getCopyBuffer()
	defer putCopyBuffer(bufPtr) // 将缓冲区返还给池
	buf := *bufPtr

	_, err := io.CopyBuffer(w, src, buf) // 使用带缓冲的复制
	if err != nil {
		return err // 返回复制过程中发生的错误
	}

	// 如果使用了 maxLatencyWriter，确保在复制完成后，任何挂起的刷新都被执行
	// （尽管 mlw.stop() 可能会处理一些情况，但显式 Flush() 可能更安全，如果 mlw 内部没有在 EOF 后自动 flush）
	// 实际上，maxLatencyWriter 的设计是在每次写入后重置或启动定时器，
	// io.CopyBuffer 会进行多次 Write 调用。mlw.stop() 会停止定时器。
	// 如果最后一次 Write 后没有达到 latency 就 EOF 了，delayedFlush 可能不会被触发。
	// 但通常 io.Copy 结束后，如果需要，调用一次 Flush 是好的做法。
	// 然而，这里的 dst 可能是原始的 ResponseWriter 或 mlw。
	// 如果是 mlw，它在 stop 时会取消 flushPending。
	// 如果是原始 dst，直接 Flush。
	if flushInterval != 0 && flusher != nil {
		// 如果是 mlw 包装的，它的 delayedFlush 或 Write(-1) 应该处理了。
		// 如果是直接的 flusher，可能需要手动 flush。
		// 考虑到 io.Copy 返回后，数据已完全写入 w。
		// 如果 w 是 mlw，它在最后一次 Write 后，如果 latency < 0 会 flush。
		// 如果 latency > 0，timer 会负责。
		// 如果 io.Copy 因为 src EOF 而结束，mlw 的最后一次 Write 之后，如果 timer 还没到，
		// 且数据量不足以触发立即刷新 (对于 latency < 0 的情况)，则可能需要手动 flush。
		// 但通常 HTTP 响应结束时，服务器会自动刷新或关闭连接，这通常足够了。
		// 为简单起见，我们依赖 maxLatencyWriter 的内部逻辑和最终的连接关闭。
	}
	return nil
}

// handleProtocolUpgrade 处理协议升级响应 (如 WebSocket)。
// (确保 ascii.IsPrint 和 ascii.EqualFold 可用)
func handleProtocolUpgrade(c *touka.Context, inReq *http.Request, backendResp *http.Response) error {
	clientResWriter := c.Writer
	inReqUpgradeType := upgradeType(inReq.Header)
	backendRespUpgradeType := upgradeType(backendResp.Header)

	if !ascii.IsPrint(backendRespUpgradeType) {
		return fmt.Errorf("backend tried to switch to invalid protocol: %q", backendRespUpgradeType) // 英文错误
	}
	if !ascii.EqualFold(inReqUpgradeType, backendRespUpgradeType) { // 使用 ascii.EqualFold
		return fmt.Errorf("backend tried to switch protocol %q when %q was requested", backendRespUpgradeType, inReqUpgradeType) // 英文错误
	}

	// 后端连接现在在 backendResp.Body 中
	backendConn, ok := backendResp.Body.(io.ReadWriteCloser)
	if !ok {
		// 这不应该发生，因为 http.Transport 在收到 101 后会将底层的 net.Conn 放入 Body
		return errors.New("internal error: 101 Switching Protocols response with non-writable/closable body") // 英文错误
	}
	// 注意：backendConn (即 backendResp.Body) 的关闭责任：
	// 对于 ServeReverseProxy (自动模式), 它会安排在 ServeReverseProxyCore 返回后关闭。
	// 但由于这里是劫持，backendConn 的生命周期会更长，由下面的双向复制逻辑管理。
	// 对于 ServeReverseProxyManual (手动模式)，调用者负责关闭从 ServeReverseProxyManual 返回的 backendResp.Body。

	hijacker, ok := clientResWriter.(http.Hijacker)
	if !ok {
		return errors.New("client ResponseWriter does not support Hijack for protocol switch") // 英文错误
	}
	// 劫持与客户端的连接
	clientConn, clientBrw, err := hijacker.Hijack() // clientBrw 是 *bufio.ReadWriter
	if err != nil {
		return fmt.Errorf("failed to hijack client connection: %w", err) // 英文错误
	}
	// clientConn 也需要在完成后关闭。通常在双向复制 goroutine 结束后关闭。

	// 监听原始请求的上下文取消信号
	ctx := inReq.Context()
	var wg sync.WaitGroup // 用于等待双向复制 goroutine

	// 在函数退出时，确保清理连接（除非它们已被明确关闭）
	// 这是为了处理 handleProtocolUpgrade 本身提前退出的情况（例如写入101失败）
	// 如果双向复制启动了，它们会自己处理关闭。
	var closeClientOnce sync.Once
	var closeBackendOnce sync.Once
	defer closeClientOnce.Do(func() { clientConn.Close() })
	defer closeBackendOnce.Do(func() { backendConn.Close() })

	// 将 101 响应（包括头部）写入劫持的客户端连接的缓冲写入器
	// 确保后端响应的协议版本是正确的，以便 Write 方法能正确格式化状态行
	if backendResp.ProtoMajor == 0 && backendResp.ProtoMinor == 0 {
		backendResp.ProtoMajor = 1
		backendResp.ProtoMinor = 1
	}
	// http.Response.Write 方法会将状态行、头部写入。
	// 对于101响应，它不应该尝试写入Body。
	// 我们需要确保 backendResp 的 Body 在这里不被意外写入，
	// 但实际上，backendResp.Body 就是 backendConn，Write 方法对这种情况有特殊处理。
	// 为了安全，可以临时设置 Body 为 nil，但通常不需要。
	if err = backendResp.Write(clientBrw); err != nil {
		return fmt.Errorf("error writing 101 response to client: %w", err) // 英文错误
	}
	if err = clientBrw.Flush(); err != nil { // 确保头部已发送到客户端
		return fmt.Errorf("error flushing 101 response to client: %w", err) // 英文错误
	}

	// 启动双向复制数据
	errChan := make(chan error, 2) // 缓冲为2，因为有两个 goroutine 可能发送错误

	wg.Add(2)
	go func() { // 后端 -> 客户端 (写入 clientConn)
		defer wg.Done()
		defer closeClientOnce.Do(func() { clientConn.Close() })   // 复制结束或出错时关闭客户端连接
		defer closeBackendOnce.Do(func() { backendConn.Close() }) // 也尝试关闭后端，以中断另一个方向

		// 从 backendConn 读取，写入 clientConn (因为 clientBrw.Writer 是 clientConn)
		_, copyErr := io.Copy(clientConn, backendConn)
		if copyErr != nil && !isNetErrClosed(copyErr) {
			errChan <- fmt.Errorf("copy from backend to client failed: %w", copyErr) // 英文错误
		}
	}()

	go func() { // 客户端 -> 后端 (写入 backendConn)
		defer wg.Done()
		defer closeBackendOnce.Do(func() { backendConn.Close() }) // 复制结束或出错时关闭后端连接
		defer closeClientOnce.Do(func() { clientConn.Close() })   // 也尝试关闭客户端，以中断另一个方向

		// 从 clientConn (通过 clientBrw.Reader) 读取，写入 backendConn
		_, copyErr := io.Copy(backendConn, clientBrw.Reader) // 使用 clientBrw.Reader 读取可能已缓冲的数据
		if copyErr != nil && !isNetErrClosed(copyErr) {
			errChan <- fmt.Errorf("copy from client to backend failed: %w", copyErr) // 英文错误
		}
	}()

	// 等待双向复制完成或上下文取消或发生错误
	select {
	case <-ctx.Done():
		// 上下文取消，这会触发上面 goroutine 中的连接关闭
		log.Printf("toukautil.handleProtocolUpgrade: Context cancelled during upgrade for %s, ensuring connections are closed.", inReq.URL.Path) // 英文日志
		// 等待 goroutines 因为连接关闭而退出
		wg.Wait()
		return fmt.Errorf("context cancelled during upgrade: %w", ctx.Err()) // 英文错误
	case err = <-errChan: // 第一个发生的复制错误
		// 一个方向出错，另一个方向的 goroutine 应该也会因为连接关闭而很快结束
		log.Printf("toukautil.handleProtocolUpgrade: Data copy error during upgrade: %v", err) // 英文日志
		wg.Wait()                                                                              // 等待两个 goroutine 都结束
		return err                                                                             // 返回捕获到的第一个错误
	case <-func() chan struct{} { // 等待 wg.Wait() 完成的辅助select case
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		return done
	}():
		// 所有复制正常完成 (wg.Wait() 返回)
		log.Printf("toukautil.handleProtocolUpgrade: Bidirectional copy completed for %s", inReq.URL.Path) // 英文日志
		// 检查是否有在 wg.Wait() 之后，但在 select case 被选中之前发生的错误或上下文取消
		select {
		case err = <-errChan: // 捕获可能在等待期间发生的错误
			if err != nil {
				log.Printf("toukautil.handleProtocolUpgrade: Late data copy error: %v", err) // 英文日志
				return err
			}
		case <-ctx.Done():
			log.Printf("toukautil.handleProtocolUpgrade: Context cancelled shortly after copy completion for %s", inReq.URL.Path) // 英文日志
			return fmt.Errorf("context cancelled shortly after copy completion: %w", ctx.Err())                                   // 英文错误
		default:
			// 无进一步错误或取消
		}
		return nil
	}
}

// maxLatencyWriter (保持不变)
type maxLatencyWriter struct {
	dst          io.Writer
	flusher      http.Flusher
	latency      time.Duration
	mu           sync.Mutex // 保护 timer 和 flushPending
	timer        *time.Timer
	flushPending bool
}

func newMaxLatencyWriter(dst io.Writer, latency time.Duration, flusher http.Flusher) *maxLatencyWriter {
	return &maxLatencyWriter{
		dst:     dst,
		latency: latency,
		flusher: flusher,
	}
}

func (m *maxLatencyWriter) Write(p []byte) (n int, err error) {
	// 注意: net/http/httputil/reverseproxy.go 中的实现对这里的锁处理更精细，
	// 允许并发写入（如果dst支持），然后在操作timer时才加锁。
	// 为了简单起见，这里使用一个全局锁。如果性能是关键问题，可以参考标准库的实现。
	m.mu.Lock()
	defer m.mu.Unlock()

	n, err = m.dst.Write(p) // 执行写入
	if err != nil {
		return n, err
	}

	if m.flusher == nil { // 如果没有 flusher，直接返回写入结果
		return n, nil
	}

	if m.latency < 0 { // 负延迟表示每次写入后立即刷新
		m.flusher.Flush()
		return n, nil
	}

	// 对于正延迟 (latency > 0)
	if m.latency == 0 { // 零延迟表示不使用定时刷新，依赖外部或连接关闭时的刷新
		return n, nil
	}

	m.flushPending = true // 标记有数据等待刷新
	if m.timer == nil {   // 如果计时器未启动
		m.timer = time.AfterFunc(m.latency, m.delayedFlush)
	} else {
		m.timer.Reset(m.latency) // 重置计时器
	}
	return n, nil
}

func (m *maxLatencyWriter) delayedFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.flushPending || m.flusher == nil { // 如果没有待刷新数据或没有 flusher
		return
	}
	m.flusher.Flush()      // 执行刷新
	m.flushPending = false // 清除待刷新标记
	// 不需要停止 timer，AfterFunc 是一次性的，或者 Reset 会重用它
}

func (m *maxLatencyWriter) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false // 停止时，取消待刷新标记
	if m.timer != nil {
		m.timer.Stop() // 停止计时器
		m.timer = nil  // 清除引用，以便GC
	}
}
