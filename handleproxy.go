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
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/fenthope/toukautil/ascii"

	"github.com/WJQSERVER-STUDIO/go-utils/iox"
	"github.com/WJQSERVER-STUDIO/httpc"
	"github.com/infinite-iroha/touka"
	"golang.org/x/net/http/httpguts"
)

// --- 对象池 ---
var (
	// copyBufferPool 用于 io.CopyBuffer 操作，减少内存分配
	// 此池用于在代理服务器和客户端之间复制响应体时使用的字节缓冲区
	copyBufferPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 32*1024) // 32KB 默认缓冲区大小
			return &b
		},
	}
)

// getCopyBuffer 从池中获取一个字节切片缓冲区
func getCopyBuffer() *[]byte {
	return copyBufferPool.Get().(*[]byte)
}

// putCopyBuffer 将字节切片缓冲区返还到池中
func putCopyBuffer(buf *[]byte) {
	copyBufferPool.Put(buf)
}

// --- 辅助函数 ---

// hopHeaders 定义了不应在代理之间转发的逐跳 HTTP 头部
// 这些头部是特定于单个TCP连接的，而不是端到端的
var hopHeaders = []string{
	"Connection",          // 控制当前连接的选项
	"Proxy-Connection",    // 类似 Connection，但用于代理
	"Keep-Alive",          // HTTP/1.0 中用于保持连接的头部
	"Proxy-Authenticate",  // 代理服务器的认证质询
	"Proxy-Authorization", // 客户端到代理服务器的认证凭证
	"Te",                  // 客户端声明支持的传输编码（除 chunked 外）
	"Trailer",             // 声明响应尾部存在的头部字段
	"Transfer-Encoding",   // 指示应用于消息体的编码（如 chunked）
	"Upgrade",             // 用于请求切换到不同协议
}

// removeHopByHopHeaders 从给定的 http.Header 中移除所有逐跳头部
// 这是实现透明代理的关键步骤，确保代理不会干扰连接管理或协议特定的细节
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

// copyHeader 将所有头部从 src 复制到 dst
// 对于多值头部，它会为每个值调用 Add，从而保留所有值
func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// singleJoiningSlash 安全地拼接两个 URL 路径段 a 和 b
// 它能正确处理 a 的尾部斜杠和 b 的头部斜杠，确保结果路径只有一个斜杠分隔（如果需要）
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		if b == "" { // 如果后一个路径段为空，则直接返回前一个
			return a
		}
		return a + "/" + b // 两个都没有斜杠，中间加一个
	}
	return a + b // 一个有斜杠，一个没有，直接拼接
}

// upgradeType 从头部获取 "Upgrade" 字段的值，
// 前提是 "Connection" 头部包含 "Upgrade" token (不区分大小写)
func upgradeType(h http.Header) string {
	if !httpguts.HeaderValuesContainsToken(h["Connection"], "Upgrade") {
		return ""
	}
	return h.Get("Upgrade")
}

// cloneURL 深拷贝一个 url.URL 对象，包括 Userinfo
// 这对于防止原始 URL 对象在代理过程中被意外修改很重要
func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	// 创建一个新的 url.URL 实例
	u2 := new(url.URL)
	*u2 = *u // 复制 u 的所有值到 u2
	// 如果 User 字段非 nil，也需要深拷贝它
	if u.User != nil {
		u2.User = new(url.Userinfo)
		*u2.User = *u.User
	}
	return u2
}

// isNetErrClosed 检查错误是否表示网络连接已关闭的常见错误类型
// 这用于在双向复制数据时区分正常的连接关闭 (如 io.EOF) 和意外的网络错误
func isNetErrClosed(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error() // 获取错误消息字符串
	// 检查是否包含常见的网络关闭错误子串或是否为特定的错误类型
	return strings.Contains(s, "use of closed network connection") || strings.Contains(s, "broken pipe") || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)
}

// ResponseMiddleware 是一个在后端响应头已写入客户端，但在响应体复制之前执行的中间件类型
// 它允许在最后时刻修改客户端响应（主要是头部）或执行其他操作
// 如果中间件返回错误，将中止响应体的复制，并调用配置的 ErrorHandler
type ResponseMiddleware func(backendResp *http.Response, clientResWriter http.ResponseWriter, c *touka.Context) error

// ReverseProxyConfig 包含反向代理的配置选项
type ReverseProxyConfig struct {
	// TargetURL 是后端目标服务器的基础 URL 字符串
	// 例如 "http://localhost:8081/api" 或 "https://service.example.com"
	// 对于 HandleReverseProxy，此字段会被使用
	// 对于其他接受 targetURL 参数的函数，此字段通常会被覆盖或忽略
	TargetURL string

	// HTTPClient 是用于执行出站请求的 httpc.Client 实例
	// 如果为 nil，将尝试从 touka.Context 的 c.GetHTTPC() 中获取
	HTTPClient *httpc.Client

	// RewriteRequest 是一个可选的回调函数，在发送请求到后端之前修改出站请求
	// 参数 inReq 是原始的入站请求 (来自客户端)
	// 参数 outReqBuilder 是 httpc.RequestBuilder，用于构建出站请求
	// 如果此函数返回错误，代理将中止并调用 ErrorHandler (在自动模式下) 或返回错误 (在手动模式下)
	RewriteRequest func(inReq *http.Request, outReqBuilder *httpc.RequestBuilder) error

	// ModifyResponse 是一个可选的回调函数，在收到后端响应后、
	// 将其头部和状态码应用到客户端响应之前调用 (在自动模式下)
	// 或在返回给手动模式调用者之前调用
	// 允许检查或修改将要写回客户端的响应（主要是头部）
	// 如果此函数返回错误，代理将调用 ErrorHandler (在自动模式下) 或返回错误 (在手动模式下)
	ModifyResponse func(backendResp *http.Response) error

	// ResponseMiddlewares 是一个响应处理中间件切片
	// 它们在后端响应头和状态码已写入客户端之后、响应体开始复制之前按顺序执行
	// 注意: 此选项仅在 ServeReverseProxy (自动模式) 中生效
	ResponseMiddlewares []ResponseMiddleware

	// ErrorHandler 是一个可选函数，用于处理代理过程中发生的错误
	// 如果为 nil，将使用默认的错误处理
	// 此处理器负责向客户端发送错误响应并中止 Touka Context (如果需要)
	ErrorHandler func(c *touka.Context, err error)

	// FlushInterval 指定在复制响应体时刷新到客户端的刷新间隔
	// 零表示不周期性刷新负值表示每次写入后立即刷新
	// 对于流式响应 (ContentLength -1 或 Content-Type text/event-stream) 会被覆盖为立即刷新
	// 注意: 此选项仅在 ServeReverseProxy (自动模式) 中生效
	FlushInterval time.Duration

	// DirectUrl 跳过拼接, 传入target即是完整目标
	isDirectUrl bool
}

// defaultProxyErrorHandler 是默认的错误处理器
// 它记录错误并通过 Touka Context 返回一个 502 Bad Gateway 响应
func defaultProxyErrorHandler(c *touka.Context, err error) {
	// 使用英文记录错误日志，包含请求方法和路径
	// 已替换为 c.Errorf
	c.Errorf("toukautil.ReverseProxy: Error for request %s %s: %v", c.Request.Method, c.Request.URL.Path, err)
	if !c.Writer.Written() { // 只有在响应头还没写入时才尝试发送错误响应
		// 使用 Touka Context 的 String 方法返回 502 错误
		// 返回英文错误信息给客户端
		c.String(http.StatusBadGateway, "Proxy Error: %s", err.Error())
	}
	// 确保 Touka 的处理链被中止，防止后续 Handler 执行
	if !c.IsAborted() {
		c.Abort()
	}
}

// prepareReverseProxy 验证配置并设置必要的组件，如目标 URL、HTTP 客户端和错误处理器
// 这是一个内部辅助函数，用于初始化和验证代理操作所需的通用配置
// 它优先使用 dynamicTargetURL (如果提供)，否则使用 config.TargetURL
// 如果 config.HTTPClient 为 nil，则尝试从 c.GetHTTPC() 获取
// 如果 config.ErrorHandler 为 nil，则使用 defaultProxyErrorHandler
// 返回准备好的 target *url.URL, *httpc.Client, error handler function, 以及任何准备错误
func prepareReverseProxy(c *touka.Context, config *ReverseProxyConfig, dynamicTargetURL ...string) (target *url.URL, httpClient *httpc.Client, errorHandler func(*touka.Context, error), err error) {
	// 确定最终的目标 URL 字符串
	targetURLStr := config.TargetURL // 默认为配置中的 TargetURL
	if len(dynamicTargetURL) > 0 && dynamicTargetURL[0] != "" {
		targetURLStr = dynamicTargetURL[0] // 如果提供了动态 URL，则覆盖
	}

	// 目标 URL 必须被提供
	if targetURLStr == "" {
		err = errors.New("TargetURL is not configured and not provided dynamically") // 英文错误
		return
	}

	// 解析目标 URL
	target, err = url.Parse(targetURLStr)
	if err != nil {
		err = fmt.Errorf("failed to parse TargetURL '%s': %w", targetURLStr, err) // 英文错误
		return
	}
	// 目标 URL 必须包含 scheme (如 http, https) 和 host
	if target.Scheme == "" || target.Host == "" {
		err = fmt.Errorf("TargetURL '%s' must have a scheme and host", targetURLStr) // 英文错误
		return
	}

	// 获取或设置 HTTPClient
	httpClient = config.HTTPClient // 优先使用配置中提供的
	if httpClient == nil {
		httpClient = c.GetHTTPC() // 尝试从 Touka Context 获取
		if httpClient == nil {    // 如果两者都未提供
			err = errors.New("HTTPClient is not available from Touka Context and not provided in ReverseProxyConfig") // 英文错误
			return
		}
	}

	// 获取或设置 ErrorHandler
	errorHandler = config.ErrorHandler // 优先使用配置中提供的
	if errorHandler == nil {
		errorHandler = defaultProxyErrorHandler // 使用默认的错误处理器
	}
	return // 返回准备好的组件和 nil 错误 (如果成功)
}

// ServeReverseProxyCore 是反向代理的核心逻辑，不直接处理响应体的复制
// 它负责：
// 1. 构建出站请求 (使用 httpc.RequestBuilder)
// 2. 应用 RewriteRequest 回调
// 3. 发送请求到后端 (使用 httpc.Client)
// 4. 处理 HTTP 1xx (Continue) 响应
// 5. 如果是协议升级 (如 WebSocket)，调用 handleProtocolUpgrade
// 6. 对于普通响应，移除逐跳头部并应用 ModifyResponse 回调
//
// 返回后端响应对象 `*http.Response`（Body 尚未关闭，由调用者负责）和任何发生的错误
// 此函数更偏向于“手动模式”的构建块
func ServeReverseProxyCore(c *touka.Context, config ReverseProxyConfig, target *url.URL, httpClient *httpc.Client) (backendResp *http.Response, err error) {
	inReq := c.Request // 原始入站请求

	// 1. 使用 httpc.RequestBuilder 准备出站请求
	// 组合目标URL的基础路径和入站请求的路径
	finalTargetPath := singleJoiningSlash(target.Path, inReq.URL.Path)
	// 组合目标URL的查询参数和入站请求的查询参数
	finalTargetQuery := target.RawQuery
	inReqQuery := inReq.URL.RawQuery
	if finalTargetQuery == "" || inReqQuery == "" {
		finalTargetQuery = finalTargetQuery + inReqQuery
	} else {
		finalTargetQuery = finalTargetQuery + "&" + inReqQuery
	}
	// 处理 RawPath (已编码的路径)
	var finalRawPath string
	if target.RawPath != "" || inReq.URL.RawPath != "" {
		finalRawPath = singleJoiningSlash(target.EscapedPath(), inReq.URL.EscapedPath())
	}
	// 构建最终的出站 URL
	outURL := &url.URL{
		Scheme:   target.Scheme,
		Host:     target.Host,
		Path:     finalTargetPath,
		RawPath:  finalRawPath,
		RawQuery: finalTargetQuery,
	}

	var outboundUrl string
	if !config.isDirectUrl {
		outboundUrl = outURL.String()
	} else {
		outboundUrl = target.String()
	}

	// 创建 httpc 请求构建器
	outReqBuilder := httpClient.NewRequestBuilder(inReq.Method, outboundUrl)
	outReqBuilder.WithContext(inReq.Context()) // 传递上下文，用于超时、取消和追踪
	outReqBuilder.NoDefaultHeaders()           // 代理通常不应添加自己的默认头部 (如 User-Agent)，而是转发或按需修改

	// 准备要转发的头部：先复制所有入站头部，再处理逐跳头部和特定代理头部
	tempOutHeader := make(http.Header)
	copyHeader(tempOutHeader, inReq.Header)

	outReqOriginalUpgrade := upgradeType(tempOutHeader) // 检查原始请求是否有 Upgrade 意图
	removeHopByHopHeaders(tempOutHeader)                // 从临时头部副本中移除逐跳头部
	if outReqOriginalUpgrade != "" {                    // 如果是协议升级请求
		if !ascii.IsPrint(outReqOriginalUpgrade) { // 使用你内部的 ascii 包验证协议名
			return nil, fmt.Errorf("client tried to switch to invalid protocol %q", outReqOriginalUpgrade) // 英文错误
		}
		// 重新设置标准的 Connection 和 Upgrade 头部，以确保它们被正确转发
		tempOutHeader.Set("Connection", "Upgrade")
		tempOutHeader.Set("Upgrade", outReqOriginalUpgrade)
	}
	// 如果原始客户端请求表明支持 trailers，也通知后端
	if httpguts.HeaderValuesContainsToken(inReq.Header["Te"], "trailers") {
		tempOutHeader.Set("Te", "trailers")
	}
	// 如果原始请求没有 User-Agent，则确保出站请求的 User-Agent 为空，避免 Go 的默认值
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
		// ContentLength 由 httpc.RequestBuilder 或 http.NewRequest 内部根据 Body 类型处理
		// 如果需要精确控制，可以在 RewriteRequest 中获取最终的 *http.Request 并设置
	}

	// 调用用户提供的 RewriteRequest 回调，允许用户在发送前最后修改出站请求
	if config.RewriteRequest != nil {
		if errRewrite := config.RewriteRequest(inReq, outReqBuilder); errRewrite != nil {
			return nil, fmt.Errorf("RewriteRequest failed: %w", errRewrite) // 英文错误
		}
	}

	// 构建最终的 *http.Request 对象
	finalOutReq, buildErr := outReqBuilder.Build()
	if buildErr != nil {
		return nil, fmt.Errorf("failed to build outgoing request: %w", buildErr) // 英文错误
	}

	// 2. 发送请求到后端，并处理 1xx 响应
	var roundTripErr error
	var roundTripMutex sync.Mutex // 保护 roundTripDone 标志
	var roundTripDone bool        // 标记 RoundTrip 是否已完成，用于 ClientTrace

	// 设置 ClientTrace 以捕获 1xx (Continue) 响应
	trace := &httptrace.ClientTrace{
		Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
			roundTripMutex.Lock()
			defer roundTripMutex.Unlock()
			if roundTripDone { // 如果主 HTTP RoundTrip 已完成，则忽略此 1xx 响应
				return nil
			}
			// 将 1xx 响应头和状态码写回原始客户端
			// 注意：这会直接写入 c.Writer，对于完全手动的代理模式，用户可能不希望这样
			// 但对于 ServeReverseProxy (自动模式)，这是期望的行为
			clientResWriterHeaders := c.Writer.Header()
			copyHeader(clientResWriterHeaders, http.Header(header))
			c.Writer.WriteHeader(code)
			// 标准库的 WriteHeader 对于 1xx 响应不会自动清除已设置的头部，所以手动清除
			for k := range clientResWriterHeaders {
				delete(clientResWriterHeaders, k)
			}
			return nil
		},
	}
	// 将带有 trace 的上下文附加到出站请求
	finalOutReq = finalOutReq.WithContext(httptrace.WithClientTrace(finalOutReq.Context(), trace))

	// 使用配置的 httpc.Client 执行请求
	backendResp, roundTripErr = httpClient.Do(finalOutReq)
	roundTripMutex.Lock()
	roundTripDone = true // 标记主 RoundTrip 完成
	roundTripMutex.Unlock()

	if roundTripErr != nil {
		// 请求后端失败返回的 backendResp 此时可能为 nil，或者包含部分信息
		// 将错误和（可能存在的）响应一起返回给调用者处理
		return backendResp, fmt.Errorf("request to backend failed via httpc: %w", roundTripErr) // 英文错误
	}
	// 至此，已成功从后端获取响应头backendResp.Body 尚未关闭，由本函数的调用者负责

	// 3. 初步处理后端响应 (头部修改，协议升级判断)
	// 如果是协议升级响应 (例如 WebSocket 的 101)，则特殊处理
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// handleProtocolUpgrade 会负责处理与客户端和后端的连接劫持及数据复制
		// 它也会向客户端写入 101 响应头
		if errUpgrade := handleProtocolUpgrade(c, inReq, backendResp); errUpgrade != nil {
			// 协议升级失败返回原始的 backendResp (其 Body 是劫持的连接或已关闭) 和错误
			return backendResp, errUpgrade
		}
		// 协议升级成功backendResp.Body 现在代表与后端的劫持连接
		// ServeReverseProxyCore 的任务完成
		return backendResp, nil
	}

	// 对于非 101 的普通 HTTP 响应，移除其逐跳头部
	removeHopByHopHeaders(backendResp.Header)

	// 调用用户提供的 ModifyResponse 回调，允许修改后端响应（主要是头部）
	if config.ModifyResponse != nil {
		if errModify := config.ModifyResponse(backendResp); errModify != nil {
			// 修改响应失败返回原始的 backendResp 和错误
			return backendResp, fmt.Errorf("ModifyResponse failed: %w", errModify) // 英文错误
		}
	}
	// 返回处理过的后端响应和 nil 错误，表示核心代理步骤成功
	// backendResp.Body 仍需由调用者关闭
	return backendResp, nil
}

// ServeReverseProxyManual ("手动模式") 允许调用者更细致地控制反向代理的响应处理
// 它执行代理请求的核心逻辑 (通过 ServeReverseProxyCore)，处理协议升级，
// 并返回处理后的后端响应头和整个后端响应对象
//
// 返回值:
//   - processedBackendHeader (http.Header): 这是从后端响应中提取并处理过（移除了逐跳头部，应用了ModifyResponse）的头部
//     如果发生错误导致无法获取后端响应，此值可能为 nil
//   - backendResp (*http.Response): 从后端获取的原始（或已由ModifyResponse修改的）*http.Response 对象
//     调用者【始终负责】在处理完毕后调用 backendResp.Body.Close() 来释放资源，即使在发生错误时也是如此（如果 backendResp 非 nil）
//   - err (error): 代理过程中发生的任何错误
//
// 调用者责任:
// 1. 检查返回的 `err`
// 2. 如果 `backendResp` 非 `nil`，必须调用 `backendResp.Body.Close()`
// 3. 根据 `processedBackendHeader` 和 `backendResp` (特别是 `backendResp.Body`) 自行构建对客户端的响应
//
// 协议升级:
// 如果 `err` 为 `nil` 且 `backendResp.StatusCode` 是 `http.StatusSwitchingProtocols` (101)，
// 则表示协议升级已由 `ServeReverseProxyCore` 内部的 `handleProtocolUpgrade` 处理完毕
// `backendResp.Body` 将是与后端服务器的劫持连接 (`io.ReadWriteCloser`)
// 调用者此时通常不需要再对客户端做 HTTP 响应，因为连接已被劫持用于新协议
// 调用者仍需关闭这个 `backendResp.Body` (劫持的连接) 当它不再需要时
func ServeReverseProxyManual(c *touka.Context, config ReverseProxyConfig, dynamicTargetURL ...string) (processedBackendHeader http.Header, backendResp *http.Response, err error) {
	// 初始化并验证代理配置
	target, httpClient, _, prepErr := prepareReverseProxy(c, &config, dynamicTargetURL...)
	if prepErr != nil {
		// 返回 nil, nil 和准备错误
		return nil, nil, fmt.Errorf("failed to prepare reverse proxy for manual serve: %w", prepErr) // 英文错误
	}

	// 调用核心代理逻辑
	// 注意：errorHandler 参数在这里传递的是 prepareReverseProxy 返回的（可能是默认的），
	// 但 ServeReverseProxyCore 在手动模式下不应主动调用它，而是返回错误
	// errorHandler 的主要作用是在自动模式 (ServeReverseProxy) 中
	// 这里传递 nil 也可以，因为 ServeReverseProxyCore 不会调用它（它返回错误）
	// 为了保持 ServeReverseProxyCore 的签名一致，我们仍然传递它
	coreResp, coreErr := ServeReverseProxyCore(c, config, target, httpClient)

	// 无论 ServeReverseProxyCore 是否成功，如果它返回了一个 coreResp，
	// 这个 coreResp (特别是它的 Body) 的关闭责任现在就属于 ServeReverseProxyManual 的调用者了
	// 我们将 coreResp 直接赋给 backendResp 返回

	if coreErr != nil {
		// 核心逻辑出错
		// 即使出错，coreResp 也可能包含一些信息（例如部分读取的响应头）
		// 将 coreResp 和 coreErr 一起返回
		var hdr http.Header
		if coreResp != nil {
			hdr = coreResp.Header // 如果响应对象存在，头部也可能存在
		}
		return hdr, coreResp, fmt.Errorf("ServeReverseProxyCore error in manual serve: %w", coreErr) // 英文错误
	}

	// 此时 coreResp 是有效的后端响应 (或协议升级后的响应)
	// coreResp.Header 已经是处理过的头部
	return coreResp.Header, coreResp, nil
}

// ServeReverseProxy ("自动模式") 对给定的 Touka Context 执行完整的反向代理操作
// 它处理从构建请求到将响应体完全复制回客户端的所有步骤，包括错误处理和中间件
func ServeReverseProxy(c *touka.Context, config ReverseProxyConfig, dynamicTargetURL ...string) {
	// 初始化并验证代理配置，获取目标URL、HTTP客户端和错误处理器
	target, httpClient, errorHandler, err := prepareReverseProxy(c, &config, dynamicTargetURL...)
	if err != nil {
		// 如果配置准备阶段就出错，则调用错误处理器
		errorHandler(c, fmt.Errorf("failed to prepare reverse proxy for auto serve: %w", err)) // 英文错误
		return
	}

	// 调用核心代理逻辑获取后端响应
	// ServeReverseProxyCore 返回的 backendResp 的 Body 需要在本函数内负责关闭
	backendResp, coreErr := ServeReverseProxyCore(c, config, target, httpClient)
	// 使用 defer 确保 backendResp.Body 在函数退出前被关闭（除非是协议升级且连接已被完全劫持和管理）
	if backendResp != nil && backendResp.Body != nil {
		defer func() {
			// 尝试消耗剩余的 Body 内容，有助于连接复用
			_, _ = iox.Copy(io.Discard, backendResp.Body)
			backendResp.Body.Close()
		}()
	}

	if coreErr != nil {
		// 核心代理逻辑发生错误错误处理器负责处理并中止 Touka Context
		errorHandler(c, fmt.Errorf("ServeReverseProxyCore failed in auto serve: %w", coreErr)) // 英文错误
		return
	}

	// 检查是否是协议升级响应 (例如 WebSocket 101)
	if backendResp.StatusCode == http.StatusSwitchingProtocols {
		// 如果是协议升级，ServeReverseProxyCore 内部的 handleProtocolUpgrade
		// 已经处理了与客户端的连接劫持和101响应头的发送
		// backendResp.Body 此时代表与后端的劫持连接，其生命周期由 handleProtocolUpgrade 管理
		// ServeReverseProxy 在这里的工作已经完成
		return
	}

	// --- 对于非协议升级的成功响应 ---

	// 将后端响应头复制到客户端 ResponseWriter
	copyHeader(c.Writer.Header(), backendResp.Header) // backendResp.Header 已被 ServeReverseProxyCore 清理和修改

	// 处理响应 Trailer 声明：如果后端声明了 Trailer，也告知客户端
	announcedTrailers := len(backendResp.Trailer)
	if announcedTrailers > 0 {
		trailerKeys := make([]string, 0, len(backendResp.Trailer))
		for k := range backendResp.Trailer {
			trailerKeys = append(trailerKeys, k)
		}
		c.Writer.Header().Add("Trailer", strings.Join(trailerKeys, ", "))
	}

	// 写入状态码到客户端这之后就不能再修改响应头了
	c.Writer.WriteHeader(backendResp.StatusCode)

	// 执行响应处理中间件这些中间件在状态码和头部已写入客户端后，但在响应体复制前执行
	// 它们可以进行日志记录，或通过 c.Writer (如果它支持某些接口) 做一些非常有限的修改，
	// 但主要是用于观察或触发基于响应头的副作用
	if len(config.ResponseMiddlewares) > 0 {
		for _, mw := range config.ResponseMiddlewares {
			// 将 Touka Context 传递给响应中间件，以便它们可以访问请求信息或中止 Context
			if errMw := mw(backendResp, c.Writer, c); errMw != nil {
				// 如果响应中间件返回错误，调用主错误处理器
				// 此时响应头和状态码已发送，错误处理器主要能做的就是记录错误和中止后续操作
				errorHandler(c, fmt.Errorf("ResponseMiddleware failed: %w", errMw)) // 英文错误
				return                                                              // 中止后续响应体复制
			}
			// 如果中间件调用了 c.Abort()，我们也应该中止
			if c.IsAborted() {
				// 已替换为 c.Infof
				c.Infof("toukautil.ServeReverseProxy: ResponseMiddleware aborted the context.")
				return
			}
		}
	}

	// 复制响应体到客户端
	// backendResp.Body 将在 defer 中关闭
	flushInterval := determineActualFlushInterval(backendResp, config.FlushInterval)
	if copyErr := copyProxiedResponseBody(c.Writer, backendResp.Body, flushInterval); copyErr != nil {
		// 记录复制响应体时的错误此时头部和状态码已发送
		// 常见的错误是客户端断开连接 (isNetErrClosed 会捕获 io.EOF 或 "broken pipe")
		// 或者上下文被取消 (例如，Touka 的超时中间件触发)
		// 只有在不是正常的网络关闭或上下文取消时才打印详细日志
		if !isNetErrClosed(copyErr) && !errors.Is(copyErr, c.Request.Context().Err()) {
			// 使用英文记录日志
			// 已替换为 c.Errorf
			c.Errorf("toukautil.ServeReverseProxy: Error copying response body for %s %s: %v", c.Request.Method, c.Request.URL.Path, copyErr)
		}
		// errorHandler 在这里不适合调用，因为它可能会尝试再次写入响应头
		// Touka 的 Context 可能会因客户端断开而自动中止
		// 确保 Touka 的 Recovery 中间件能处理这里的 panic (如果 copyProxiedResponseBody 内部 panic 的话)
		return
	}

	// 复制实际的 Trailer 头部（如果后端发送了且与声明的一致）
	if len(backendResp.Trailer) > 0 && len(backendResp.Trailer) == announcedTrailers {
		copyHeader(c.Writer.Header(), backendResp.Trailer)
	} else if len(backendResp.Trailer) > 0 { // 如果后端发送了未在 Trailer 头部声明的 Trailer
		// (这通常不符合规范，但以防万一)，按 TrailerPrefix 添加
		for k, vv := range backendResp.Trailer {
			k = http.TrailerPrefix + k // 例如 "Trailer:X-My-Trailer"
			for _, v := range vv {
				c.Writer.Header().Add(k, v)
			}
		}
	}
	// 代理操作完成Touka 的处理链会自然结束，或被之前的 c.Abort() 中止
}

// HandleReverseProxy 实现了一个便捷的 Touka Handler，用于将请求完整地反向代理
// 它内部调用 ServeReverseProxy，并使用 config 中定义的 TargetURL
// 这个函数适用于在定义 Touka 路由时，直接指定一个完整的反向代理行为
//
// 示例用法：
//
//	r.GET("/proxy-to-service-a/*path", func(c *touka.Context) {
//	    config := toukautil.ReverseProxyConfig{ TargetURL: "http://service-a-backend" }
//	    toukautil.HandleReverseProxy(c, config)
//	})
func HandleReverseProxy(c *touka.Context, config ReverseProxyConfig) {
	ServeReverseProxy(c, config) // 直接调用全自动模式的 ServeReverseProxy
}

// SingleReverseProxy 将当前 Touka Context 中的请求反向代理到指定的 `targetURL`
// 它使用传入的 `config` 作为基础配置，但 `targetURL` 参数会覆盖 `config.TargetURL`
// 这个函数适用于那些目标 URL 需要在运行时动态确定的场景，
// 例如，基于请求的某些参数或头部来选择不同的后端服务
//
// 示例用法：
//
//	r.GET("/dynamic-proxy", func(c *touka.Context) {
//	    targetService := c.Query("service_id") // 从查询参数获取目标
//	    targetServiceURL := "http://backend-" + targetService + ".internal"
//	    baseConfig := toukautil.ReverseProxyConfig{ /* ... 一些通用配置 ... */ }
//	    toukautil.SingleReverseProxy(targetServiceURL, c, baseConfig)
//	})
func SingleReverseProxy(targetURL string, c *touka.Context, config ReverseProxyConfig) {
	// ServeReverseProxy 的第三个参数 (dynamicTargetURL) 用于传递这个动态 URL
	ServeReverseProxy(c, config, targetURL)
}

// SimpleSingleReverseProxy 提供了一个最简单的接口来反向代理到一个完整的 URL
// 它使用默认的配置，只要求提供目标 URL 和 Touka Context
// 适用于快速、无特殊配置的单个 URL 代理场景
//
// 示例用法：
//
//	r.GET("/quick-proxy/*path", func(c *touka.Context) {
//	    toukautil.SimpleSingleReverseProxy("http://another-service.com", c)
//	})
func SimpleSingleReverseProxy(targetURL string, c *touka.Context) {
	// 使用一个空的 ReverseProxyConfig，让 ServeReverseProxy 使用所有默认值
	// (除了 TargetURL，它会被 dynamicTargetURL 参数覆盖)
	config := ReverseProxyConfig{}
	ServeReverseProxy(c, config, targetURL)
}

// DirectSingleReverseProxy 提供了一个简单的接口来反向代理到一个完整的 URL，
// 并且允许传入一个临时的 ReverseProxyConfig 来覆盖默认配置
// 与 SimpleSingleReverseProxy 不同，它不使用完全默认配置，而是基于传入的 tempConfig
// 同时，它设置 config.isDirectUrl = true，表示 targetURL 参数就是完整的后端 URL，
// 不需要再与原始请求路径拼接
//
// 示例用法：
//
//	r.GET("/direct-proxy", func(c *touka.Context) {
//	    targetURL := c.Query("target") // 从查询参数获取完整的后端 URL
//	    if targetURL == "" {
//	        c.String(http.StatusBadRequest, "Missing target URL")
//	        return
//	    }
//	    // 可以在这里设置一些临时的配置，例如超时时间
//	    tempConfig := toukautil.ReverseProxyConfig{
//	        HTTPClient: &http.Client{ Timeout: 10 * time.Second },
//	    }
//	    toukautil.DirectSingleReverseProxy(targetURL, &tempConfig, c)
//	})
func DirectSingleReverseProxy(targetURL string, tempConfig *ReverseProxyConfig, c *touka.Context) {
	config := *tempConfig
	config.TargetURL = targetURL
	config.isDirectUrl = true
	ServeReverseProxy(c, config, targetURL)
}

// determineActualFlushInterval 根据响应的 Content-Type 和 ContentLength
// 以及用户配置的 FlushInterval，来决定实际应该使用的刷新间隔
// 例如，对于 Server-Sent Events ("text/event-stream") 或内容长度未知的流式响应，
// 通常应立即刷新 (-1)
func determineActualFlushInterval(res *http.Response, configuredInterval time.Duration) time.Duration {
	resCT := res.Header.Get("Content-Type")
	// 解析媒体类型，忽略参数（如 charset）
	if baseCT, _, err := mime.ParseMediaType(resCT); err == nil && baseCT == "text/event-stream" {
		return -1 // 对于 Server-Sent Events，总是立即刷新
	}
	// 如果 ContentLength 未知（通常是 -1，表示流式传输），也倾向于立即刷新
	if res.ContentLength == -1 {
		return -1
	}
	// 否则，使用用户配置的间隔
	return configuredInterval
}

// copyProxiedResponseBody 将响应体从 src (后端响应体) 复制到 dst (客户端 ResponseWriter)
// 它支持通过 flushInterval 控制的周期性刷新
func copyProxiedResponseBody(dst http.ResponseWriter, src io.Reader, flushInterval time.Duration) error {
	var w io.Writer = dst // 默认直接写入 ResponseWriter
	var flusher http.Flusher

	// 检查 dst (客户端 ResponseWriter) 是否支持 http.Flusher 接口
	if f, ok := dst.(http.Flusher); ok {
		flusher = f
	}

	// 如果需要刷新 (flushInterval 非零) 且 ResponseWriter 支持刷新
	if flushInterval != 0 && flusher != nil {
		// 创建一个 maxLatencyWriter 来包装原始的 ResponseWriter，
		// 它会根据 flushInterval 自动或周期性地调用 Flush
		mlw := newMaxLatencyWriter(dst, flushInterval, flusher)
		defer mlw.stop() // 确保 maxLatencyWriter 的资源（如定时器）被释放
		w = mlw          // 将写入目标切换到 maxLatencyWriter
	}

	bufPtr := getCopyBuffer()            // 从池中获取缓冲区
	defer putCopyBuffer(bufPtr)          // 将缓冲区返还给池
	buf := *bufPtr                       // 解引用指针以获取字节切片
	_, err := io.CopyBuffer(w, src, buf) // 使用带缓冲的复制
	if err != nil {
		return err // 返回复制过程中发生的任何错误
	}
	return nil
}

// handleProtocolUpgrade 处理协议升级响应 (如 WebSocket 的 101 Switching Protocols)
// 它负责：
// 1. 验证客户端请求的升级协议与后端响应的升级协议是否匹配
// 2. 劫持客户端连接 (通过 clientResWriter.Hijack())
// 3. 获取与后端的原始连接 (通过 backendResp.Body 类型断言)
// 4. 将 101 升级响应头写回客户端
// 5. 启动两个 goroutine 在客户端连接和后端连接之间双向复制数据
// 6. 管理这些 goroutine 的生命周期和错误处理
// (此函数与你提供的最新版本基本一致，确保 ascii.IsPrint 和 ascii.EqualFold 可用)
func handleProtocolUpgrade(c *touka.Context, inReq *http.Request, backendResp *http.Response) error {
	clientResWriter := c.Writer                               // 客户端的 ResponseWriter
	inReqUpgradeType := upgradeType(inReq.Header)             // 客户端请求的 Upgrade 类型
	backendRespUpgradeType := upgradeType(backendResp.Header) // 后端响应的 Upgrade 类型

	// 使用你内部的 ascii 包进行验证
	if !ascii.IsPrint(backendRespUpgradeType) { // 检查后端响应的协议名是否可打印
		return fmt.Errorf("backend tried to switch to invalid protocol: %q", backendRespUpgradeType) // 英文错误
	}
	if !ascii.EqualFold(inReqUpgradeType, backendRespUpgradeType) { // 检查协议是否匹配 (不区分大小写)
		return fmt.Errorf("backend tried to switch protocol %q when %q was requested", backendRespUpgradeType, inReqUpgradeType) // 英文错误
	}

	// 后端连接应该在 backendResp.Body 中，并且它必须是一个 io.ReadWriteCloser
	backendConn, ok := backendResp.Body.(io.ReadWriteCloser)
	if !ok {
		// 这通常不应该发生，因为标准库的 http.Transport 在收到 101 响应后，
		// 会将底层的 net.Conn 放入响应的 Body 字段
		return errors.New("internal error: 101 Switching Protocols response with non-writable/closable body") // 英文错误
	}
	// backendConn (即 backendResp.Body) 的关闭责任现在由这个函数或其调用的 goroutine 管理

	// 尝试劫持客户端连接
	hijacker, ok := clientResWriter.(http.Hijacker)
	if !ok { // 如果客户端 ResponseWriter 不支持 Hijack
		backendConn.Close()                                                                    // 既然无法劫持客户端，就关闭到后端的连接
		return errors.New("client ResponseWriter does not support Hijack for protocol switch") // 英文错误
	}
	clientConn, clientBrw, err := hijacker.Hijack() // clientBrw 是 *bufio.ReadWriter
	if err != nil {
		backendConn.Close()                                              // 劫持失败，关闭后端连接
		return fmt.Errorf("failed to hijack client connection: %w", err) // 英文错误
	}
	// clientConn 也需要在完成后关闭我们使用 defer clientConn.Close()，
	// 但双向复制的 goroutine 也会在结束时尝试关闭它

	// 为了确保在各种情况下（包括 panic）连接都能被关闭，使用 defer
	// 但由于双向复制 goroutine 也会关闭连接，需要小心处理 "use of closed network connection" 错误
	// closeClientOnce 和 closeBackendOnce 用于确保每个连接只被这个 defer 栈尝试关闭一次
	var closeClientOnce sync.Once
	var closeBackendOnce sync.Once
	defer closeClientOnce.Do(func() { clientConn.Close() })
	defer closeBackendOnce.Do(func() { backendConn.Close() })

	// 监听原始请求的上下文取消信号
	ctx := inReq.Context() // 获取请求的上下文
	// 如果上下文被取消，我们希望尽早关闭两个连接以释放资源
	if ctx.Done() != nil { // 检查上下文是否可被取消
		stopWatcher := make(chan struct{}) // 用于信号通知 watcher goroutine 停止
		defer close(stopWatcher)           // 确保 watcher goroutine 退出

		go func() {
			select {
			case <-ctx.Done(): // 上下文被取消
				// 已替换为 c.Infof
				c.Infof("toukautil.handleProtocolUpgrade: Context cancelled, closing connections for %s.", inReq.URL.Path)
				closeClientOnce.Do(func() { clientConn.Close() })
				closeBackendOnce.Do(func() { backendConn.Close() })
			case <-stopWatcher: // handleProtocolUpgrade 正常退出
				return
			}
		}()
	}

	// 将 101 Switching Protocols 响应（包括头部）写入劫持的客户端连接的缓冲写入器
	// 确保后端响应的协议版本被正确设置，以便 http.Response.Write 能正确格式化状态行
	if backendResp.ProtoMajor == 0 && backendResp.ProtoMinor == 0 {
		backendResp.ProtoMajor = 1
		backendResp.ProtoMinor = 1
	}
	// http.Response.Write 方法会将状态行和头部写入
	// 对于 101 响应，它不应该尝试写入 Body (因为 Body 是 nil，或者被视为劫持连接)
	if err = backendResp.Write(clientBrw); err != nil {
		return fmt.Errorf("error writing 101 response to client: %w", err) // 英文错误
	}
	if err = clientBrw.Flush(); err != nil { // 确保头部已实际发送到客户端
		return fmt.Errorf("error flushing 101 response to client: %w", err) // 英文错误
	}

	// 启动双向数据复制
	errChan := make(chan error, 1) // 缓冲为1，用于捕获第一个发生的复制错误
	var wg sync.WaitGroup
	wg.Add(2) // 等待两个复制 goroutine 完成

	// Goroutine: 从后端复制数据到客户端
	go func() {
		defer wg.Done()                                           // 标记此 goroutine 完成
		defer closeClientOnce.Do(func() { clientConn.Close() })   // 确保客户端连接在其使用完毕后关闭
		defer closeBackendOnce.Do(func() { backendConn.Close() }) // 如果这边先出错，也关闭后端连接

		// 从 backendConn 读取，写入 clientConn (因为 clientBrw.Writer 底层是 clientConn)
		_, copyErr := iox.Copy(clientConn, backendConn)
		if copyErr != nil && !isNetErrClosed(copyErr) { // 忽略正常的 EOF 或连接关闭错误
			// 只有在发生意外错误时才发送到 errChan
			select {
			case errChan <- fmt.Errorf("copy from backend to client failed: %w", copyErr): // 英文错误
			default: // 如果 errChan 已满 (已被其他错误填充)，则忽略此错误
			}
		}
	}()

	// Goroutine: 从客户端复制数据到后端
	go func() {
		defer wg.Done()                                           // 标记此 goroutine 完成
		defer closeBackendOnce.Do(func() { backendConn.Close() }) // 确保后端连接在其使用完毕后关闭
		defer closeClientOnce.Do(func() { clientConn.Close() })   // 如果这边先出错，也关闭客户端连接

		// 从 clientConn (通过 clientBrw.Reader 以处理已缓冲数据) 读取，写入 backendConn
		_, copyErr := iox.Copy(backendConn, clientBrw.Reader)
		if copyErr != nil && !isNetErrClosed(copyErr) { // 忽略正常的 EOF 或连接关闭错误
			select {
			case errChan <- fmt.Errorf("copy from client to backend failed: %w", copyErr): // 英文错误
			default:
			}
		}
	}()

	// 等待所有复制完成，或第一个错误发生，或上下文取消
	waitAllCopiesDone := make(chan struct{})
	go func() {
		wg.Wait()                // 等待两个 io.Copy goroutine 都完成
		close(waitAllCopiesDone) // 然后关闭此 channel 以发出信号
	}()

	select {
	case err = <-errChan: // 捕获第一个发生的实际I/O复制错误
		// 如果 errChan 收到错误，它会首先被选中
		if err != nil { // 仅当确实是错误时（非nil，尽管 isNetErrClosed 可能已过滤）
			c.Errorf("toukautil.handleProtocolUpgrade: Data copy error during upgrade: %v", err) // 英文日志
			return err                                                                           // 返回捕获到的第一个错误
		}
		// 如果 err 是 nil (例如，一个 copy goroutine 正常结束发送 nil 到 errChan，但这是不期望的)
		// 此时，我们应该等待 waitAllCopiesDone 或 ctx.Done()
		select {
		case <-waitAllCopiesDone: // 另一个也完成了
			c.Infof("toukautil.handleProtocolUpgrade: Bidirectional copy completed (one side reported nil error) for %s", inReq.URL.Path) // 英文日志
			return nil
		case <-ctx.Done(): // 上下文在等待期间取消
			c.Infof("toukautil.handleProtocolUpgrade: Context cancelled while waiting after one-sided nil error for %s", inReq.URL.Path) // 英文日志
			return fmt.Errorf("context cancelled during upgrade wait: %w", ctx.Err())                                                    // 英文错误
		}
	case <-waitAllCopiesDone: // 所有复制正常完成 (wg.Wait() 返回，没有错误发送到 errChan)
		c.Infof("toukautil.handleProtocolUpgrade: Bidirectional copy completed successfully for %s", inReq.URL.Path) // 英文日志
		// 此时，仍需检查在 wg.Wait() 完成后，是否有非常晚的错误或上下文取消
		// （这种概率较小，但为了健壮性可以检查）
		select {
		case err = <-errChan: // 捕获可能在等待期间发生的、但未被第一个 select case 选中的错误
			if err != nil {
				c.Errorf("toukautil.handleProtocolUpgrade: Late data copy error after wg.Wait: %v", err) // 英文日志
				return err
			}
		case <-ctx.Done(): // 上下文在所有复制完成后、此select case前被取消
			c.Infof("toukautil.handleProtocolUpgrade: Context cancelled shortly after copy completion for %s", inReq.URL.Path) // 英文日志
			return fmt.Errorf("context cancelled shortly after copy completion: %w", ctx.Err())                                // 英文错误
		default:
			// 无进一步错误或取消
		}
		return nil // 一切正常
	case <-ctx.Done(): // 上下文在任何复制错误或所有复制完成之前被取消
		c.Infof("toukautil.handleProtocolUpgrade: Context cancelled during upgrade data copy for %s", inReq.URL.Path) // 英文日志
		// 在返回前，等待复制 goroutine 因上下文取消或连接关闭而终止
		<-waitAllCopiesDone                                                            // 这确保了 goroutines 不会泄漏
		return fmt.Errorf("context cancelled during upgrade data copy: %w", ctx.Err()) // 英文错误
	}
}

// maxLatencyWriter 用于控制响应体复制时的刷新行为
// 它包装了一个 io.Writer (通常是 http.ResponseWriter) 和一个 http.Flusher
// 如果 latency < 0，则每次 Write 后立即 Flush
// 如果 latency > 0，则在每次 Write 后，如果在 latency 时间内没有新的 Write，则调用 Flush
// 如果 latency == 0，则不进行基于延迟的 Flush (依赖外部或连接关闭时的刷新)
type maxLatencyWriter struct {
	dst     io.Writer     // 实际写入的目标
	flusher http.Flusher  // 用于刷新缓冲区的接口
	latency time.Duration // 刷新的最大延迟

	mu           sync.Mutex  // 保护 timer 和 flushPending
	timer        *time.Timer // 用于延迟刷新的定时器
	flushPending bool        // 标记是否有数据等待刷新
}

// newMaxLatencyWriter 创建一个新的 maxLatencyWriter 实例
// dst 是最终的写入目标
// latency 是刷新延迟
// flusher 是从 dst 获取的 http.Flusher (如果 dst 支持)
func newMaxLatencyWriter(dst io.Writer, latency time.Duration, flusher http.Flusher) *maxLatencyWriter {
	return &maxLatencyWriter{
		dst:     dst,
		latency: latency,
		flusher: flusher, // 存储传入的 flusher
	}
	// 定时器将在第一次符合条件的 Write 操作时启动
}

// Write 将数据写入 dst，并根据 latency 和 flusher 的存在来管理刷新
func (m *maxLatencyWriter) Write(p []byte) (n int, err error) {
	// 注意: 标准库 httputil 中的 maxLatencyWriter 在这里的锁处理更复杂，
	// 它允许 dst.Write 在锁外执行（如果 dst 是线程安全的），
	// 仅在操作共享状态 (timer, flushPending) 时加锁
	// Touka 的 ResponseWriter 通常不是设计为并发写入的，所以这里的全局锁是合适的
	m.mu.Lock()
	defer m.mu.Unlock()

	n, err = m.dst.Write(p) // 首先执行实际的写入操作
	if err != nil {
		return n, err // 如果写入出错，直接返回错误
	}

	if m.flusher == nil { // 如果没有可用的 flusher，则无法执行刷新
		return n, nil
	}

	if m.latency < 0 { // 负延迟表示每次写入后立即刷新
		m.flusher.Flush() // 执行刷新，忽略刷新错误（通常难处理）
		return n, nil
	}

	// 对于正延迟 (latency > 0)
	if m.latency == 0 { // 零延迟表示不使用基于此 writer 的定时刷新
		return n, nil
	}

	m.flushPending = true // 标记有数据写入，等待刷新
	if m.timer == nil {   // 如果计时器当前未运行（例如第一次写入，或上次已触发）
		// 创建一个新的定时器，在 latency 时间后调用 delayedFlush
		m.timer = time.AfterFunc(m.latency, m.delayedFlush)
	} else {
		// 如果计时器已存在，则重置它，推迟下一次 delayedFlush 的调用
		m.timer.Reset(m.latency)
	}
	return n, nil
}

// delayedFlush 是由 time.AfterFunc 调用的函数，用于执行延迟刷新
func (m *maxLatencyWriter) delayedFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 再次检查 flushPending，因为在 timer 触发和此函数获取锁之间，
	// stop() 可能已经被调用，或者新的 Write 可能已经重置了 timer
	if !m.flushPending || m.flusher == nil {
		return
	}
	m.flusher.Flush()      // 执行实际的刷新操作
	m.flushPending = false // 清除待刷新标记
	// AfterFunc 是一次性的，下次需要时会重新创建或 Reset
}

// stop 停止 maxLatencyWriter 的任何挂起的刷新操作，并释放其资源（如定时器）
// 通常在代理完成响应体复制后由 defer调用
func (m *maxLatencyWriter) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false // 取消任何待处理的刷新标记
	if m.timer != nil {
		m.timer.Stop() // 停止计时器，防止它再次触发
		m.timer = nil  // 清除对计时器的引用，帮助垃圾回收
	}
}
