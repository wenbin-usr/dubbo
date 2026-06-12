# Dubbo 协议（TCP）深入源码分析

## 一、模块定位与整体架构

Dubbo 协议（又称 Dubbo2 协议）是 Dubbo 框架的原生 TCP 协议，基于长连接 + NIO + 自定义二进制协议头实现。核心实现位于两个模块：

| 模块 | 职责 |
|------|------|
| `dubbo-rpc/dubbo-rpc-dubbo` | 协议层：`DubboProtocol`、`DubboInvoker`、`DubboCodec`、编解码 |
| `dubbo-remoting/dubbo-remoting-api` | 传输层：`ExchangeCodec`、`HeaderExchangeHandler`、`DefaultFuture`、心跳 |

### 核心类关系

```mermaid
classDiagram
    class DubboProtocol {
        -Map~String,SharedClientsProvider~ referenceClientMap
        -ExchangeHandler requestHandler
        +export(Invoker) Exporter
        +refer(Class, URL) Invoker
        -createServer(URL) ProtocolServer
        -getClients(URL) ClientsProvider
        -getInvoker(Channel, Invocation) Invoker
    }
    class DubboInvoker {
        -ClientsProvider clientsProvider
        -AtomicPositiveInteger index
        +doInvoke(Invocation) Result
    }
    class DubboCodec {
        +decodeBody(Channel, InputStream, byte[]) Object
        -encodeRequestData()
        -encodeResponseData()
    }
    class DecodeableRpcInvocation {
        -Channel channel
        -InputStream inputStream
        -Request request
        +decode()
        +decode(Channel, InputStream) Object
    }
    class DecodeableRpcResult {
        -Channel channel
        -Response response
        -Invocation invocation
        +decode()
        +decode(Channel, InputStream) Object
    }
    class ExchangeCodec {
        -short MAGIC = 0xdabb
        -int HEADER_LENGTH = 16
        +encode(Channel, ChannelBuffer, Object)
        +decode(Channel, ChannelBuffer) Object
        -encodeRequest(Channel, ChannelBuffer, Request)
        -encodeResponse(Channel, ChannelBuffer, Response)
    }
    class HeaderExchangeHandler {
        -ExchangeHandler handler
        +received(Channel, Object)
        -handleRequest(ExchangeChannel, Request)
        -handleResponse(Channel, Response)
    }
    class HeaderExchangeChannel {
        -Channel channel
        +request(Object, int, ExecutorService) CompletableFuture
        +send(Object, boolean)
    }
    class DefaultFuture {
        -Long id
        -Channel channel
        -Request request
        -int timeout
        +newFuture(Channel, Request, int, ExecutorService) DefaultFuture
        +received(Channel, Response)
        +getFuture(long) DefaultFuture
    }
    class DubboCountCodec {
        -DubboCodec codec
        +encode(Channel, ChannelBuffer, Object)
        +decode(Channel, ChannelBuffer) Object
    }
    class HeartbeatHandler {
        +received(Channel, Object)
        +sent(Channel, Object)
    }

    AbstractProtocol <|-- DubboProtocol
    AbstractInvoker <|-- DubboInvoker
    ExchangeCodec <|-- DubboCodec
    DubboProtocol --> DubboInvoker : refer()
    DubboProtocol --> ExchangeCodec : codec
    DubboInvoker --> HeaderExchangeChannel : request()
    DubboInvoker --> DefaultFuture : newFuture()
    HeaderExchangeHandler --> DefaultFuture : received()
    HeaderExchangeHandler --> DubboProtocol : reply()
    DubboCodec --> DecodeableRpcInvocation : decode request
    DubboCodec --> DecodeableRpcResult : decode response
    DubboCountCodec --> DubboCodec : wraps
    HeartbeatHandler --> HeaderExchangeHandler : delegates
```

### 整体架构分层

```mermaid
graph TB
    subgraph "RPC 层"
        A[DubboProtocol<br/>服务导出/引用]
        B[DubboInvoker<br/>客户端调用器]
        C[DecodeableRpcInvocation<br/>请求解码]
        D[DecodeableRpcResult<br/>响应解码]
    end

    subgraph "Exchange 层"
        E[HeaderExchangeChannel<br/>请求/响应通道]
        F[HeaderExchangeHandler<br/>消息分发处理]
        G[DefaultFuture<br/>异步响应等待]
        H[HeaderExchangeClient<br/>客户端连接管理]
    end

    subgraph "Transport 层"
        I[NettyTransporter<br/>Netty 传输实现]
        J[NettyServer / NettyClient<br/>服务端/客户端]
        K[HeartbeatHandler<br/>心跳检测]
    end

    subgraph "Codec 层"
        L[DubboCountCodec<br/>多消息编解码]
        M[DubboCodec<br/>Dubbo 协议编解码]
        N[ExchangeCodec<br/>通用交换编解码]
    end

    A --> B
    B --> E
    B --> G
    E --> H
    H --> I
    I --> J
    J --> K
    K --> F
    F --> A
    A --> L
    L --> M
    M --> N
```

---

## 二、Dubbo 协议帧格式

### 2.1 协议头结构（16 字节）

Dubbo 协议采用定长 16 字节 Header + 变长 Body 的帧格式：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Magic Number (0xdabb)    |  Flag  |  Status  | RequestID |
|       (2 bytes)                | (1 B)  |  (1 B)   |  (8 B)    |
|                               |        |          |           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Data Length (4 bytes)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                    Body (variable length)                      |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**字段详解：**

| 偏移 | 长度 | 字段 | 说明 |
|------|------|------|------|
| 0 | 2 bytes | Magic Number | 魔数 `0xdabb`，用于协议识别 |
| 2 | 1 byte | Flag | 标志位：bit7=请求/响应, bit6=双向/单向, bit5=事件, bit0-4=序列化ID |
| 3 | 1 byte | Status | 响应状态码（仅响应有效） |
| 4 | 8 bytes | Request ID | 请求唯一ID，用于请求-响应匹配 |
| 12 | 4 bytes | Data Length | Body 长度（字节） |
| 16 | variable | Body | 序列化后的消息体 |

### 2.2 Flag 标志位

```mermaid
flowchart LR
    subgraph "Flag Byte (1 byte)"
        B7["bit7<br/>FLAG_REQUEST<br/>0x80"]
        B6["bit6<br/>FLAG_TWOWAY<br/>0x40"]
        B5["bit5<br/>FLAG_EVENT<br/>0x20"]
        B4["bit4-0<br/>SERIALIZATION_MASK<br/>0x1f"]
    end
    
    B7 -->|"1 = 请求<br/>0 = 响应"| R1[请求/响应]
    B6 -->|"1 = 双向<br/>0 = 单向"| R2[双向/单向]
    B5 -->|"1 = 事件<br/>0 = 正常"| R3[心跳/事件]
    B4 -->|"序列化类型ID"| R4["2=hessian2<br/>6=fastjson2<br/>等"]
```

源码定义在 [`ExchangeCodec`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/codec/ExchangeCodec.java)：

```java
protected static final short MAGIC = (short) 0xdabb;
protected static final int HEADER_LENGTH = 16;
protected static final byte FLAG_REQUEST = (byte) 0x80;   // 请求标记
protected static final byte FLAG_TWOWAY = (byte) 0x40;    // 双向标记
protected static final byte FLAG_EVENT = (byte) 0x20;     // 事件标记（心跳等）
protected static final int SERIALIZATION_MASK = 0x1f;     // 序列化掩码
```

### 2.3 协议帧编码流程

```mermaid
flowchart TD
    A[消息对象<br/>Request / Response] --> B{消息类型?}
    B -->|Request| C[encodeRequest]
    B -->|Response| D[encodeResponse]
    
    C --> C1[写入 Magic 0xdabb]
    C1 --> C2["设置 Flag<br/>(FLAG_REQUEST | serializationId)"]
    C2 --> C3{isTwoWay?}
    C3 -->|是| C4["Flag |= FLAG_TWOWAY"]
    C3 -->|否| C5[跳过]
    C4 --> C6{isEvent?}
    C5 --> C6
    C6 -->|是| C7["Flag |= FLAG_EVENT"]
    C6 -->|否| C8[跳过]
    C7 --> C9[写入 RequestID 8字节]
    C8 --> C9
    C9 --> C10[预留 Header 16字节空间]
    C10 --> C11[序列化 Body]
    C11 --> C12[计算 Body 长度]
    C12 --> C13[回填 DataLength 到 Header]
    C13 --> C14[写入 ChannelBuffer]
    
    D --> D1[写入 Magic 0xdabb]
    D1 --> D2["设置 Flag<br/>(serializationId)"]
    D2 --> D3{isHeartbeat?}
    D3 -->|是| D4["Flag |= FLAG_EVENT"]
    D3 -->|否| D5[跳过]
    D4 --> D6[设置 Status]
    D5 --> D6
    D6 --> D7[写入 RequestID 8字节]
    D7 --> D8[预留 Header 16字节空间]
    D8 --> D9{Status == OK?}
    D9 -->|是| D10[序列化响应数据]
    D9 -->|否| D11[序列化错误信息]
    D10 --> D12[计算 Body 长度并回填]
    D11 --> D12
    D12 --> D13[写入 ChannelBuffer]
```

---

## 三、编解码实现详解

### 3.1 编解码器层次结构

```mermaid
flowchart TD
    A[DubboCountCodec<br/>多消息编解码] --> B[DubboCodec<br/>Dubbo 协议编解码]
    B --> C[ExchangeCodec<br/>通用交换编解码]
    C --> D[TelnetCodec<br/>Telnet 编解码]
    
    B --> B1["decodeBody()<br/>解析 Body 部分"]
    B1 --> B2{Flag 判断}
    B2 -->|"请求 (FLAG_REQUEST=1)"| B3[DecodeableRpcInvocation<br/>解码 RpcInvocation]
    B2 -->|"响应 (FLAG_REQUEST=0)"| B4[DecodeableRpcResult<br/>解码 AppResponse]
    
    B3 --> B3a["readUTF: dubboVersion"]
    B3a --> B3b["readUTF: path"]
    B3b --> B3c["readUTF: version"]
    B3c --> B3d["readUTF: methodName"]
    B3d --> B3e["readUTF: parameterTypesDesc"]
    B3e --> B3f["readObject: args[]"]
    B3f --> B3g["readAttachments: Map"]
    
    B4 --> B4a["readByte: resultFlag"]
    B4a --> B4b{Flag 判断}
    B4b -->|"RESPONSE_VALUE (1)"| B4c["readObject: 返回值"]
    B4b -->|"RESPONSE_WITH_EXCEPTION (0)"| B4d["readThrowable: 异常"]
    B4b -->|"RESPONSE_NULL_VALUE (2)"| B4e["返回 null"]
    B4b -->|"带 ATTACHMENTS (3/4/5)"| B4f["额外 readAttachments"]
```

### 3.2 DubboCountCodec — 多消息编解码

[`DubboCountCodec`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboCountCodec.java) 是对 `DubboCodec` 的包装，支持在一次 TCP 读取中解析多条消息（TCP 粘包场景）：

```java
public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
    int save = buffer.readerIndex();
    MultiMessage result = MultiMessage.create();
    do {
        Object obj = codec.decode(channel, buffer);
        if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {
            buffer.readerIndex(save);
            break;
        } else {
            result.addMessage(obj);
            save = buffer.readerIndex();
        }
    } while (true);
    if (result.isEmpty()) return Codec2.DecodeResult.NEED_MORE_INPUT;
    if (result.size() == 1) return result.get(0);
    return result;
}
```

### 3.3 ExchangeCodec.decode() — 协议头解析

[`ExchangeCodec.decode()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/codec/ExchangeCodec.java) 核心流程：

1. **魔数校验**：检查前 2 字节是否为 `0xdabb`，不是则降级到 Telnet 协议
2. **长度检查**：可读字节 < 16，返回 `NEED_MORE_INPUT`
3. **读取 DataLength**：从 offset 12 读取 4 字节大端 int
4. **完整帧检查**：可读字节 < 16 + DataLength，返回 `NEED_MORE_INPUT`
5. **调用 decodeBody()** 解析 Body

### 3.4 DubboCodec.decodeBody() — Body 解析

[`DubboCodec.decodeBody()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboCodec.java) 根据 Flag 区分请求/响应：

**请求解码**：创建 `DecodeableRpcInvocation`，支持**延迟解码**（可在 IO 线程或业务线程解码）

**响应解码**：创建 `DecodeableRpcResult`，通过 `getRequestData()` 从 `DefaultFuture` 获取原始请求的 `Invocation` 对象，用于确定返回值类型。

### 3.5 响应结果标志位

[`DecodeableRpcResult`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DecodeableRpcResult.java) 解码响应 Body 时，第一个字节是结果类型标志：

| Flag | 常量 | 含义 |
|------|------|------|
| 0 | `RESPONSE_WITH_EXCEPTION` | 异常响应 |
| 1 | `RESPONSE_VALUE` | 正常返回值 |
| 2 | `RESPONSE_NULL_VALUE` | 返回 null |
| 3 | `RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS` | 异常 + 附件 |
| 4 | `RESPONSE_VALUE_WITH_ATTACHMENTS` | 返回值 + 附件 |
| 5 | `RESPONSE_NULL_VALUE_WITH_ATTACHMENTS` | null + 附件 |

---

## 四、客户端调用流程

### 4.1 完整调用时序图

```mermaid
sequenceDiagram
    participant App as 业务代码
    participant DI as DubboInvoker
    participant HEC as HeaderExchangeChannel
    participant HEClient as HeaderExchangeClient
    participant DF as DefaultFuture
    participant Codec as DubboCountCodec
    participant Netty as Netty Channel
    participant Server as 服务端

    App->>DI: doInvoke(invocation)
    DI->>DI: 选择 ExchangeClient<br/>(多连接时轮询)
    DI->>DI: 计算超时时间
    DI->>DI: 构建 Request 对象<br/>setData(inv) setTwoWay(true)
    
    alt 单向调用 (oneway)
        DI->>HEC: send(request, isSent)
        HEC->>Netty: channel.send(request)
        Netty-->>Server: TCP 发送
        DI-->>App: AsyncRpcResult (空结果)
    else 双向调用 (twoway)
        DI->>DF: newFuture(channel, request, timeout, executor)
        DF->>DF: FUTURES.put(id, this)
        DF->>DF: timeoutCheck(future)<br/>启动超时定时器
        DI->>HEC: request(request, timeout, executor)
        HEC->>HEClient: request(request, timeout, executor)
        HEClient->>Netty: channel.send(request)
        Netty-->>Server: TCP 发送
        DI-->>App: AsyncRpcResult(appResponseFuture)
        
        Note over Server: 服务端处理...
        
        Server-->>Netty: TCP 响应
        Netty->>Codec: decode(buffer)
        Codec->>Codec: DubboCodec.decodeBody()
        Codec->>Codec: 创建 DecodeableRpcResult
        Codec-->>Netty: Response 对象
        Netty->>Netty: HeaderExchangeHandler.received()
        Netty->>DF: DefaultFuture.received(channel, response)
        DF->>DF: FUTURES.remove(id)
        DF->>DF: 取消超时定时器
        DF->>DF: DecodeableRpcResult.decode()
        DF->>DF: complete(response.getResult())
        DF-->>App: future.get() 返回结果
    end
```

### 4.2 DubboInvoker.doInvoke() 详解

[`DubboInvoker.doInvoke()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboInvoker.java) 是客户端调用核心：

```java
protected Result doInvoke(final Invocation invocation) throws Throwable {
    RpcInvocation inv = (RpcInvocation) invocation;
    inv.setAttachment(PATH_KEY, getUrl().getPath());
    inv.setAttachment(VERSION_KEY, version);

    // 1. 选择连接（多连接时轮询）
    ExchangeClient currentClient;
    List<? extends ExchangeClient> exchangeClients = clientsProvider.getClients();
    if (exchangeClients.size() == 1) {
        currentClient = exchangeClients.get(0);
    } else {
        currentClient = exchangeClients.get(index.getAndIncrement() % exchangeClients.size());
    }

    // 2. 构建 Request
    Request request = new Request();
    request.setData(inv);
    request.setVersion(Version.getProtocolVersion());

    if (isOneway) {
        // 3a. 单向调用：发送即返回
        request.setTwoWay(false);
        currentClient.send(request, isSent);
        return AsyncRpcResult.newDefaultAsyncResult(invocation);
    } else {
        // 3b. 双向调用：发送并等待响应
        request.setTwoWay(true);
        ExecutorService executor = getCallbackExecutor(getUrl(), inv);
        CompletableFuture<AppResponse> appResponseFuture =
                currentClient.request(request, timeout, executor)
                        .thenApply(AppResponse.class::cast);
        AsyncRpcResult result = new AsyncRpcResult(appResponseFuture, inv);
        result.setExecutor(executor);
        return result;
    }
}
```

### 4.3 连接管理

```mermaid
flowchart TD
    A[DubboProtocol.refer] --> B{connections 配置?}
    B -->|"connections=0<br/>(默认共享)"| C[getSharedClient]
    B -->|"connections=N<br/>(独占连接)"| D[getExclusiveClients]
    
    C --> C1["referenceClientMap.compute<br/>按 address 缓存"]
    C1 --> C2{已存在?}
    C2 -->|是| C3["increaseCount()<br/>引用计数+1"]
    C2 -->|否| C4["创建 SharedClientsProvider<br/>默认共享连接数"]
    C3 --> C5[返回共享连接]
    C4 --> C5
    
    D --> D1["IntStream.range(0, N)<br/>创建 N 个独立连接"]
    D1 --> D2["initClient(url)<br/>逐个创建 ExchangeClient"]
    D2 --> D3[返回 ExclusiveClientsProvider]
```

### 4.4 DefaultFuture — 异步响应等待

[`DefaultFuture`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/support/DefaultFuture.java) 是 Dubbo 协议异步响应的核心机制：

```mermaid
flowchart TD
    A["DubboInvoker 发起调用"] --> B["DefaultFuture.newFuture()"]
    B --> C["FUTURES.put(id, this)<br/>注册到全局 Map"]
    C --> D["timeoutCheck(future)<br/>启动 HashedWheelTimer 超时检测"]
    D --> E["返回 CompletableFuture<br/>给上层"]
    
    F["服务端响应到达"] --> G["HeaderExchangeHandler<br/>handleResponse()"]
    G --> H["DefaultFuture.received(channel, response)"]
    H --> I["FUTURES.remove(id)<br/>从 Map 移除"]
    I --> J["取消超时定时器"]
    J --> K["DecodeableRpcResult.decode()<br/>反序列化响应"]
    K --> L["future.complete(result)<br/>通知等待方"]
    
    M["超时触发"] --> N["TimeoutCheckTask.run()"]
    N --> O["FUTURES.remove(id)"]
    O --> P["future.completeExceptionally<br/>(TimeoutException)"]
```

核心源码：

```java
// 发送请求时创建 Future
public static DefaultFuture newFuture(Channel channel, Request request, int timeout, ExecutorService executor) {
    final DefaultFuture future = new DefaultFuture(channel, request, timeout);
    future.setExecutor(executor);
    timeoutCheck(future);  // 启动超时检测
    return future;
}

// 收到响应时通知 Future
public static void received(Channel channel, Response response) {
    DefaultFuture future = FUTURES.remove(response.getId());
    if (future != null) {
        future.doReceived(response);
    }
}

private void doReceived(Response res) {
    if (timeoutCheckTask != null) {
        timeoutCheckTask.cancel();  // 取消超时检测
    }
    // 在业务线程池中解码并完成 Future
    executor.execute(() -> {
        // 解码响应
        decodeResponse(res);
        // 完成 CompletableFuture
        complete(res.getResult());
    });
}
```

---

## 五、服务端处理流程

### 5.1 服务端启动

```mermaid
flowchart TD
    A[DubboProtocol.export] --> B[openServer]
    B --> C["URL 参数设置<br/>codec=dubbo<br/>heartbeat=60000"]
    C --> D["Exchangers.bind(url, requestHandler)"]
    D --> E["HeaderExchanger.bind()"]
    E --> F["Transporters.bind()"]
    F --> G["NettyTransporter.bind()"]
    G --> H["创建 NettyServer"]
    H --> I["配置 Netty Pipeline"]
    I --> J["绑定端口 20880"]
    
    subgraph "Netty Pipeline"
        K[NettyCodecAdapter<br/>编码器]
        L[NettyCodecAdapter<br/>解码器]
        M[HeartbeatHandler<br/>心跳处理]
        N[HeaderExchangeHandler<br/>消息分发]
        O[NettyServerHandler<br/>最终处理器]
    end
    
    J --> K --> L --> M --> N --> O
```

### 5.2 服务端请求处理

```mermaid
sequenceDiagram
    participant Netty as Netty Channel
    participant Codec as DubboCountCodec
    participant HH as HeartbeatHandler
    participant HEH as HeaderExchangeHandler
    participant DP as DubboProtocol<br/>requestHandler
    participant Invoker as 业务 Invoker

    Netty->>Codec: channelRead(buffer)
    Codec->>Codec: decode() 解析协议帧
    Codec->>Codec: DubboCodec.decodeBody()
    Codec->>Codec: 创建 DecodeableRpcInvocation
    Codec-->>HH: Request 对象
    
    HH->>HH: 更新 READ_TIMESTAMP
    HH->>HH: {是否是心跳请求?}
    
    alt 心跳请求
        HH->>HH: 构建 HeartBeatResponse
        HH->>Netty: channel.send(response)
    else 业务请求
        HH->>HEH: received(channel, request)
        HEH->>HEH: handleRequest(channel, request)
        
        alt 请求解码失败 (broken)
            HEH->>Netty: send BAD_REQUEST response
        else 正常请求
            HEH->>DP: handler.reply(channel, msg)
            DP->>DP: getInvoker(channel, inv)<br/>根据 path/version/group 查找
            DP->>Invoker: invoker.invoke(inv)
            Invoker-->>DP: Result
            DP-->>HEH: CompletableFuture
            HEH->>HEH: future.whenComplete()
            HEH->>HEH: 构建 Response (OK/SERVICE_ERROR)
            HEH->>Netty: channel.send(response)
        end
    end
```

### 5.3 DubboProtocol.requestHandler — 核心请求处理器

[`DubboProtocol`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboProtocol.java) 内部定义了 `ExchangeHandlerAdapter`：

```java
requestHandler = new ExchangeHandlerAdapter(frameworkModel) {
    @Override
    public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) {
        Invocation inv = (Invocation) message;
        // 1. 根据 channel + invocation 查找 Invoker
        Invoker<?> invoker = inv.getInvoker() == null 
            ? getInvoker(channel, inv) 
            : inv.getInvoker();
        
        // 2. 设置 TCCL
        Thread.currentThread().setContextClassLoader(
            invoker.getUrl().getServiceModel().getClassLoader());
        
        // 3. 设置远程地址
        RpcContext.getServiceContext().setRemoteAddress(channel.getRemoteAddress());
        
        // 4. 调用业务 Invoker
        Result result = invoker.invoke(inv);
        return result.thenApply(Function.identity());
    }
};
```

### 5.4 Invoker 查找逻辑

[`DubboProtocol.getInvoker()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboProtocol.java#L290-L320) 通过 `serviceKey` 从 `exporterMap` 中查找：

```java
Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
    int port = channel.getLocalAddress().getPort();
    String path = (String) inv.getObjectAttachmentWithoutConvert(PATH_KEY);
    
    // 回调服务特殊处理
    boolean isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;
    if (isCallBackServiceInvoke) {
        path += "." + inv.getObjectAttachmentWithoutConvert(CALLBACK_SERVICE_KEY);
    }
    
    // 构建 serviceKey: {group}/{path}:{version}:{port}
    String serviceKey = serviceKey(port, path, 
        (String) inv.getObjectAttachmentWithoutConvert(VERSION_KEY),
        (String) inv.getObjectAttachmentWithoutConvert(GROUP_KEY));
    
    DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);
    if (exporter == null) {
        throw new RemotingException("Not found exported service: " + serviceKey);
    }
    return exporter.getInvoker();
}
```

---

## 六、心跳机制

### 6.1 心跳流程

```mermaid
sequenceDiagram
    participant Client as HeaderExchangeClient
    participant ClientTimer as HeartbeatTimerTask
    participant Server as 服务端
    participant ServerHH as HeartbeatHandler

    Note over Client,Server: 心跳检测机制

    ClientTimer->>ClientTimer: 定时检查<br/>lastReadTime / lastWriteTime
    
    alt 读空闲超时
        ClientTimer->>Client: 判定读超时
        Note over Client: 读超时 > heartbeatTimeout<br/>→ 判定连接不可用
    end
    
    alt 写空闲超时
        ClientTimer->>Client: 判定写超时
        Client->>Server: 发送 HeartBeatRequest<br/>(twoWay=true)
        Server->>ServerHH: received(heartbeat)
        ServerHH->>ServerHH: 检测到心跳请求
        ServerHH->>Server: 发送 HeartBeatResponse
        Server-->>Client: HeartBeatResponse
        Client->>Client: 更新 lastReadTime
    end
```

### 6.2 HeartbeatHandler 实现

[`HeartbeatHandler`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/support/header/HeartbeatHandler.java) 拦截所有消息：

```java
public void received(Channel channel, Object message) throws RemotingException {
    setReadTimestamp(channel);  // 更新读时间戳
    
    if (isHeartbeatRequest(message)) {
        HeartBeatRequest req = (HeartBeatRequest) message;
        if (req.isTwoWay()) {
            // 双向心跳：回复 HeartBeatResponse
            HeartBeatResponse res = new HeartBeatResponse(req.getId(), req.getVersion());
            res.setEvent(HEARTBEAT_EVENT);
            channel.send(res);
        }
        return;  // 心跳不向下传递
    }
    if (isHeartbeatResponse(message)) {
        return;  // 心跳响应不向下传递
    }
    handler.received(channel, message);  // 业务消息向下传递
}

public void sent(Channel channel, Object message) throws RemotingException {
    setWriteTimestamp(channel);  // 更新写时间戳
    handler.sent(channel, message);
}
```

### 6.3 心跳定时任务

`HeaderExchangeClient` 启动两个定时任务：

- **HeartbeatTimerTask**：检测写空闲，发送心跳请求
- **ReconnectTimerTask**：检测读空闲，触发重连

---

## 七、完整通信流程总结

### 7.1 端到端数据流

```mermaid
flowchart LR
    subgraph "客户端"
        A[业务调用] --> B[DubboInvoker]
        B --> C[构建 Request<br/>setData + setTwoWay]
        C --> D[HeaderExchangeChannel.request]
        D --> E[DefaultFuture.newFuture<br/>注册 + 超时检测]
        E --> F[Netty 发送]
    end
    
    subgraph "网络"
        F -->|TCP| G
    end
    
    subgraph "服务端"
        G[Netty 接收] --> H[DubboCountCodec.decode]
        H --> I[ExchangeCodec.decode<br/>魔数校验 + 帧解析]
        I --> J[DubboCodec.decodeBody<br/>Flag 判断]
        J --> K[DecodeableRpcInvocation<br/>反序列化参数]
        K --> L[HeartbeatHandler<br/>心跳过滤]
        L --> M[HeaderExchangeHandler<br/>handleRequest]
        M --> N[DubboProtocol.requestHandler<br/>查找 Invoker]
        N --> O[业务 Invoker.invoke]
        O --> P[构建 Response]
        P --> Q[ExchangeCodec.encodeResponse]
        Q --> R[Netty 发送]
    end
    
    subgraph "网络"
        R -->|TCP| S
    end
    
    subgraph "客户端"
        S[Netty 接收] --> T[DubboCountCodec.decode]
        T --> U[DubboCodec.decodeBody<br/>Flag 判断]
        U --> V[DecodeableRpcResult<br/>反序列化结果]
        V --> W[HeaderExchangeHandler<br/>handleResponse]
        W --> X[DefaultFuture.received<br/>匹配 Request ID]
        X --> Y[future.complete<br/>通知调用方]
        Y --> Z[业务获取结果]
    end
```

### 7.2 关键设计特点总结

| 特性 | 实现方式 |
|------|----------|
| **传输协议** | TCP 长连接 + Netty NIO |
| **协议格式** | 16 字节定长 Header + 变长 Body |
| **魔数** | `0xdabb`，用于协议识别 |
| **请求-响应匹配** | 8 字节 Request ID（AtomicLong 自增） |
| **异步响应** | `DefaultFuture`（CompletableFuture + HashedWheelTimer 超时） |
| **序列化** | Flag 低 5 位标识序列化类型（hessian2=2, fastjson2=6 等） |
| **心跳** | `HeartbeatHandler` + `HeartbeatTimerTask`，读写时间戳检测 |
| **连接管理** | 共享连接（引用计数）+ 独占连接两种模式 |
| **多消息解码** | `DubboCountCodec` 支持 TCP 粘包场景 |
| **延迟解码** | `DecodeableRpcInvocation`/`DecodeableRpcResult` 支持 IO 线程或业务线程解码 |
| **调用模式** | 双向（twoway）+ 单向（oneway） |
| **默认端口** | 20880 |

### 7.3 与 Triple 协议对比

| 维度 | Dubbo 协议 | Triple 协议 |
|------|-----------|-------------|
| 传输层 | TCP 长连接 | HTTP/2 |
| 协议格式 | 自定义二进制 16B Header | gRPC 兼容帧格式 |
| 多路复用 | 单连接串行（Request ID 匹配） | HTTP/2 Stream 原生多路复用 |
| 流式调用 | 不支持 | 支持（4 种模式） |
| 序列化 | hessian2/fastjson2 等 | Protobuf + Dubbo 序列化 |
| 跨语言 | 仅 Java | 支持（兼容 gRPC） |
| 端口 | 20880 | 50051 |
| 心跳 | 自定义双向心跳 | HTTP/2 PING 帧 |

---

## 八、核心源码文件索引

| 文件 | 职责 |
|------|------|
| `DubboProtocol.java` | 协议入口，服务导出/引用，连接管理，请求处理 |
| `DubboInvoker.java` | 客户端调用器，发起 RPC 调用 |
| `DubboCodec.java` | Dubbo 协议编解码，Body 解析 |
| `DubboCountCodec.java` | 多消息编解码包装器 |
| `DecodeableRpcInvocation.java` | 请求反序列化（延迟解码） |
| `DecodeableRpcResult.java` | 响应反序列化（延迟解码） |
| `ExchangeCodec.java` | 通用交换编解码，协议头解析 |
| `HeaderExchangeHandler.java` | 消息分发处理（请求/响应/事件） |
| `HeaderExchangeChannel.java` | Exchange 通道，request/send 封装 |
| `HeaderExchangeClient.java` | 客户端连接管理，心跳/重连定时器 |
| `DefaultFuture.java` | 异步响应等待，超时检测 |
| `HeartbeatHandler.java` | 心跳检测与处理 |
| `Request.java` | 请求模型（ID、版本、双向标记） |
| `Response.java` | 响应模型（状态码、结果、错误信息） |
| `DubboWireProtocol.java` | 端口统一复用时的 Dubbo 协议配置 |
| `DubboDetector.java` | Dubbo 协议检测器 |
| `ReferenceCountExchangeClient.java` | 引用计数连接客户端 |
| `SharedClientsProvider.java` | 共享连接提供者 |
| `CallbackServiceCodec.java` | 回调服务编解码 |
