# Dubbo 消费方订阅提供方实例 — 以 Nacos 注册中心为例

## 一、场景设定

以如下典型配置为例，分析 Dubbo 消费方如何通过 Nacos 注册中心订阅提供方实例：

**消费方配置 (consumer)：**
```properties
dubbo.application.name=dubbo-consumer-demo
dubbo.registry.address=nacos://127.0.0.1:8848
```

**提供方配置 (provider)：**
```properties
dubbo.application.name=dubbo-provider-demo
dubbo.registry.address=nacos://127.0.0.1:8848
dubbo.protocol.name=dubbo
dubbo.protocol.port=20880
```

**服务接口：**
```java
public interface DemoService {
    String sayHello(String name);
}
```

---

## 二、整体架构概览

### 2.1 核心模块与类关系

```mermaid
classDiagram
    class Registry {
        <<interface>>
        +register(URL)
        +unregister(URL)
        +subscribe(URL, NotifyListener)
        +unsubscribe(URL, NotifyListener)
        +lookup(URL) List~URL~
    }
    class AbstractRegistry {
        -Properties properties
        -Set~URL~ registered
        -ConcurrentMap~URL,Set~NotifyListener~~ subscribed
        -ConcurrentMap~URL,Map~String,List~URL~~~ notified
        +register(URL)
        +subscribe(URL, NotifyListener)
        +notify(URL, NotifyListener, List~URL~)
    }
    class FailbackRegistry {
        -ConcurrentMap failedRegistered
        -ConcurrentMap failedSubscribed
        -HashedWheelTimer retryTimer
        +register(URL)
        +subscribe(URL, NotifyListener)
        #doRegister(URL)
        #doSubscribe(URL, NotifyListener)
    }
    class NacosRegistry {
        -NacosNamingServiceWrapper namingService
        -Map originToAggregateListener
        +doRegister(URL)
        +doSubscribe(URL, NotifyListener)
        -subscribeEventListener()
        -notifySubscriber()
    }
    class RegistryProtocol {
        -Protocol protocol
        -ProxyFactory proxyFactory
        +refer(Class, URL) Invoker
        +export(Invoker) Exporter
        -doCreateInvoker()
    }
    class DynamicDirectory {
        -Cluster cluster
        -Registry registry
        -Protocol protocol
        -URL directoryUrl
        +subscribe(URL)
        +notify(List~URL~)
    }
    class RegistryDirectory {
        -Map~URL,Invoker~ urlInvokerMap
        -Set~URL~ cachedInvokerUrls
        +subscribe(URL)
        +notify(List~URL~)
        -refreshInvoker(List~URL~)
        -toInvokers(Map, List) Map
    }
    class NotifyListener {
        <<interface>>
        +notify(List~URL~)
    }
    class NacosAggregateListener {
        -NotifyListener listener
        -Set~String~ serviceNames
    }
    class NacosNamingServiceWrapper {
        +registerInstance()
        +subscribe()
        +getAllInstances()
    }

    Registry <|-- AbstractRegistry
    AbstractRegistry <|-- FailbackRegistry
    FailbackRegistry <|-- NacosRegistry
    RegistryProtocol --> Registry : getRegistry()
    RegistryProtocol --> DynamicDirectory : doCreateInvoker()
    DynamicDirectory <|-- RegistryDirectory
    DynamicDirectory --> Registry : registry
    DynamicDirectory --> NotifyListener : implements
    NacosRegistry --> NacosAggregateListener : creates
    NacosRegistry --> NacosNamingServiceWrapper : namingService
```

### 2.2 整体架构分层

```mermaid
graph TB
    subgraph "配置层"
        A[RegistryConfig<br/>nacos://127.0.0.1:8848]
    end

    subgraph "Registry 协议层"
        B[RegistryProtocol<br/>refer/export 编排]
        C[RegistryDirectory<br/>订阅 + 通知处理]
        D[DynamicDirectory<br/>抽象 Directory]
    end

    subgraph "Registry 抽象层"
        E[FailbackRegistry<br/>失败重试]
        F[AbstractRegistry<br/>本地缓存 + 通知]
    end

    subgraph "Nacos 实现层"
        G[NacosRegistry<br/>Nacos 注册中心实现]
        H[NacosNamingServiceWrapper<br/>Nacos SDK 封装]
        I[NacosAggregateListener<br/>聚合监听器]
    end

    subgraph "Nacos 服务端"
        J[Nacos Server<br/>服务注册与发现]
    end

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    G --> I
    H --> J
    I --> J
```

---

## 三、消费方订阅完整流程

### 3.1 端到端时序图

```mermaid
sequenceDiagram
    participant App as 业务代码
    participant RefBean as ReferenceBean
    participant RP as RegistryProtocol
    participant RD as RegistryDirectory
    participant FB as FailbackRegistry
    participant NR as NacosRegistry
    participant NW as NacosNamingServiceWrapper
    participant Nacos as Nacos Server

    Note over App,Nacos: === 阶段1: 服务引用触发 ===
    App->>RefBean: getObject() 获取代理
    RefBean->>RP: refer(type, registryUrl)
    Note over RP: registryUrl = registry://127.0.0.1:8848/...<br/>?registry=nacos
    
    Note over App,Nacos: === 阶段2: 创建 RegistryDirectory ===
    RP->>RP: doRefer(cluster, registry, type, url)
    RP->>RP: getRegistry(registryUrl)
    RP->>NR: registryFactory.getRegistry(url)
    NR->>NW: createNamingService(url)
    NW-->>NR: NacosNamingServiceWrapper
    NR-->>RP: NacosRegistry 实例
    
    RP->>RD: new RegistryDirectory(type, url)
    RP->>RD: setRegistry(registry)
    RP->>RD: setProtocol(protocol)
    RP->>RD: buildRouterChain(urlToRegistry)
    
    Note over App,Nacos: === 阶段3: 发起订阅 ===
    RP->>RD: subscribe(toSubscribeUrl(url))
    RD->>RD: setSubscribeUrl(url)
    RD->>FB: registry.subscribe(url, this)
    Note over RD: this = RegistryDirectory<br/>实现了 NotifyListener
    
    FB->>FB: super.subscribe(url, listener)
    Note over FB: AbstractRegistry 记录订阅关系<br/>subscribed.put(url, listeners)
    
    FB->>NR: doSubscribe(url, listener)
    
    Note over App,Nacos: === 阶段4: Nacos 订阅 ===
    NR->>NR: 创建 NacosAggregateListener
    NR->>NR: getServiceNames(url)
    Note over NR: 构建 Nacos 服务名<br/>providers:org.apache.dubbo.demo.DemoService:1.0.0:
    
    NR->>NW: getAllInstancesWithoutSubscription(serviceName, group)
    NW->>Nacos: GET /nacos/v1/ns/instance/list
    Nacos-->>NW: 实例列表 [192.168.1.100:20880, ...]
    NW-->>NR: List~Instance~
    
    NR->>NR: notifySubscriber(url, serviceName, listener, instances)
    NR->>NR: buildURLs(url, instances)
    Note over NR: 将 Nacos Instance 转换为 Dubbo URL<br/>dubbo://192.168.1.100:20880/...
    
    NR->>FB: notify(url, listener, urls)
    FB->>FB: AbstractRegistry.notify()
    Note over FB: 1. 保存到本地缓存文件<br/>2. 调用 listener.notify(urls)
    
    FB->>RD: notify(urls)
    
    Note over App,Nacos: === 阶段5: 注册 Nacos 事件监听 ===
    NR->>NW: subscribe(serviceName, group, eventListener)
    NW->>Nacos: POST /nacos/v1/ns/instance/list (长轮询/UDP)
    Note over Nacos: Nacos 会推送后续变更
    
    Note over App,Nacos: === 阶段6: 刷新 Invoker 列表 ===
    RD->>RD: notify(providerUrls)
    RD->>RD: 分类 URL (providers/configurators/routers)
    RD->>RD: refreshOverrideAndInvoker(providerURLs)
    RD->>RD: refreshInvoker(urls)
    RD->>RD: toInvokers(oldMap, urls)
    Note over RD: 将 URL 转换为 Invoker<br/>protocol.refer(type, url)
    RD->>RD: destroyUnusedInvokers()
    RD->>RD: setInvokers(newInvokers)
    
    Note over RD: Directory 准备就绪<br/>后续调用从 Invoker 列表中选择
```

### 3.2 阶段详解：从 refer 到 subscribe

```mermaid
flowchart TD
    A["ReferenceBean.getObject()"] --> B["RegistryProtocol.refer(type, url)"]
    B --> C["url = getRegistryUrl(url)<br/>将 registry:// 转为<br/>service-discovery-registry://"]
    C --> D["getRegistry(url)<br/>通过 SPI 获取 NacosRegistry"]
    D --> E["doRefer(cluster, registry, type, url)"]
    E --> F["创建 RegistryDirectory(type, url)"]
    F --> G["directory.setRegistry(registry)"]
    G --> H["directory.setProtocol(protocol)"]
    H --> I["directory.buildRouterChain(url)"]
    I --> J["directory.subscribe(toSubscribeUrl(url))"]
    
    J --> K["DynamicDirectory.subscribe(url)"]
    K --> L["registry.subscribe(url, this)"]
    L --> M["FailbackRegistry.subscribe(url, listener)"]
    M --> N["AbstractRegistry.subscribe(url, listener)<br/>记录订阅关系"]
    N --> O["FailbackRegistry.doSubscribe(url, listener)"]
    O --> P["NacosRegistry.doSubscribe(url, listener)"]
    
    P --> Q["创建 NacosAggregateListener"]
    Q --> R["getServiceNames(url)<br/>构建 Nacos 服务名"]
    R --> S["namingService.getAllInstancesWithoutSubscription()"]
    S --> T["notifySubscriber()<br/>通知已有实例"]
    T --> U["subscribeEventListener()<br/>注册变更监听"]
```

---

## 四、Nacos 注册中心核心实现

### 4.1 NacosRegistryFactory — 工厂入口

[`NacosRegistryFactory`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-nacos/src/main/java/org/apache/dubbo/registry/nacos/NacosRegistryFactory.java) 通过 SPI 机制加载（`META-INF/dubbo/internal/org.apache.dubbo.registry.RegistryFactory` 中配置 `nacos=...NacosRegistryFactory`）：

```java
public class NacosRegistryFactory extends AbstractRegistryFactory {
    @Override
    protected Registry createRegistry(URL url) {
        return new NacosRegistry(url, createNamingService(url));
    }
}
```

### 4.2 NacosRegistry.doSubscribe() — 核心订阅逻辑

[`NacosRegistry.doSubscribe()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-nacos/src/main/java/org/apache/dubbo/registry/nacos/NacosRegistry.java#L256-L310) 是订阅的核心：

```java
@Override
public void doSubscribe(final URL url, final NotifyListener listener) {
    // 1. 创建聚合监听器（一个 listener 可能对应多个 Nacos serviceName）
    NacosAggregateListener nacosAggregateListener = new NacosAggregateListener(listener);
    originToAggregateListener
            .computeIfAbsent(url, k -> new ConcurrentHashMap<>())
            .put(listener, nacosAggregateListener);

    // 2. 获取 Nacos 服务名列表
    Set<String> serviceNames = getServiceNames(url, nacosAggregateListener);

    // 3. 执行订阅
    doSubscribe(url, nacosAggregateListener, serviceNames);
}

private void doSubscribe(final URL url, final NacosAggregateListener listener, final Set<String> serviceNames) {
    for (String serviceName : serviceNames) {
        // 4. 先全量拉取已有实例
        List<Instance> instances = namingService.getAllInstancesWithoutSubscription(
                serviceName, getUrl().getGroup(Constants.DEFAULT_GROUP));
        // 5. 立即通知订阅方
        notifySubscriber(url, serviceName, listener, instances);
        // 6. 注册 Nacos 事件监听（后续变更推送）
        subscribeEventListener(serviceName, url, listener);
    }
}
```

### 4.3 Nacos 服务名构建

Dubbo 在 Nacos 中的服务名格式为：

```
providers:{interfaceName}:{version}:{group}
```

例如：`providers:org.apache.dubbo.demo.DemoService:1.0.0:`

```mermaid
flowchart LR
    A["Dubbo URL<br/>interface=org.apache.dubbo.demo.DemoService<br/>version=1.0.0<br/>group="] --> B[NacosServiceName]
    B --> C["category = providers"]
    B --> D["serviceInterface = org.apache.dubbo.demo.DemoService"]
    B --> E["version = 1.0.0"]
    B --> F["group = (empty)"]
    C --> G["Nacos 服务名<br/>providers:org.apache.dubbo.demo.DemoService:1.0.0:"]
    D --> G
    E --> G
    F --> G
```

源码 [`NacosServiceName`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-nacos/src/main/java/org/apache/dubbo/registry/nacos/NacosServiceName.java)：

```java
public class NacosServiceName {
    private static final String NAME_SEPARATOR = ":";
    
    // 格式: category:interface:version:group
    // 例如: providers:com.xxx.DemoService:1.0.0:
    public static NacosServiceName valueOf(String serviceName) {
        String[] segments = serviceName.split(NAME_SEPARATOR, -1);
        // segments[0] = category (providers/consumers/configurators/routers)
        // segments[1] = interface
        // segments[2] = version
        // segments[3] = group
    }
}
```

### 4.4 Instance 与 URL 的转换

```mermaid
flowchart LR
    subgraph "Nacos Instance"
        A1["ip: 192.168.1.100"]
        A2["port: 20880"]
        A3["metadata: {<br/>  side=provider,<br/>  application=dubbo-provider-demo,<br/>  dubbo=2.7.22,<br/>  ...<br/>}"]
    end
    
    subgraph "Dubbo URL"
        B1["protocol: dubbo"]
        B2["host: 192.168.1.100"]
        B3["port: 20880"]
        B4["parameters: {<br/>  side=provider,<br/>  application=dubbo-provider-demo,<br/>  ...<br/>}"]
    end
    
    A1 --> B2
    A2 --> B3
    A3 --> B4
    A1 --> B1
```

Nacos 的 `Instance.metadata` 中存储了 Dubbo URL 的所有参数，消费方收到 Instance 后重建为 `dubbo://192.168.1.100:20880/org.apache.dubbo.demo.DemoService?...` 格式的 URL。

### 4.5 Nacos 事件监听 — 变更推送

```mermaid
sequenceDiagram
    participant NR as NacosRegistry
    participant NW as NacosNamingServiceWrapper
    participant Nacos as Nacos Server
    participant RD as RegistryDirectory

    Note over NR,Nacos: 初始订阅完成后，注册事件监听
    
    NR->>NW: subscribe(serviceName, group, eventListener)
    NW->>Nacos: POST /nacos/v1/ns/instance/list<br/>(长轮询 或 UDP 推送)
    
    Note over Nacos: 提供方实例变更...
    
    Nacos-->>NW: NamingEvent<br/>instances: [新实例列表]
    NW-->>NR: EventListener.onEvent(event)
    
    NR->>NR: NacosAggregateListener<br/>收集所有 serviceName 的变更
    NR->>NR: notifySubscriber(url, serviceName, listener, instances)
    NR->>NR: buildURLs(url, instances)
    NR->>RD: notify(url, listener, urls)
    
    RD->>RD: AbstractRegistry.notify()
    RD->>RD: 保存到本地缓存文件
    RD->>RD: listener.notify(urls)
    
    RD->>RD: RegistryDirectory.notify(urls)
    RD->>RD: 分类 URL (providers/configurators/routers)
    RD->>RD: refreshInvoker(providerURLs)
    Note over RD: 增量更新 Invoker 列表<br/>新建/销毁 Invoker
```

---

## 五、RegistryDirectory — 通知处理与 Invoker 刷新

### 5.1 notify() 方法详解

[`RegistryDirectory.notify()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/integration/RegistryDirectory.java#L200-L240) 是消费方收到注册中心通知后的核心处理方法：

```java
@Override
public synchronized void notify(List<URL> urls) {
    // 1. 按 category 分类 URL
    Map<String, List<URL>> categoryUrls = urls.stream()
            .filter(Objects::nonNull)
            .filter(this::isValidCategory)
            .collect(Collectors.groupingBy(this::judgeCategory));

    // 2. 处理 configurators（配置覆盖规则）
    List<URL> configuratorURLs = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, ...);
    this.configurators = Configurator.toConfigurators(configuratorURLs).orElse(this.configurators);

    // 3. 处理 routers（路由规则）
    List<URL> routerURLs = categoryUrls.getOrDefault(ROUTERS_CATEGORY, ...);
    toRouters(routerURLs).ifPresent(this::addRouters);

    // 4. 处理 providers（提供方地址列表）
    List<URL> providerURLs = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, ...);
    
    // 5. 扩展点：AddressListener 可以对 URL 做二次处理
    List<AddressListener> supportedListeners = ...;
    for (AddressListener addressListener : supportedListeners) {
        providerURLs = addressListener.notify(providerURLs, getConsumerUrl(), this);
    }
    
    // 6. 刷新 Invoker 列表
    refreshOverrideAndInvoker(providerURLs);
}
```

### 5.2 refreshInvoker() — Invoker 增量更新

```mermaid
flowchart TD
    A["refreshInvoker(List~URL~ invokerUrls)"] --> B{"invokerUrls 是否为空?"}
    
    B -->|"是 (空列表)"| C{"cachedInvokerUrls<br/>是否有缓存?"}
    C -->|是| D["使用缓存 URL<br/>触发空保护"]
    C -->|否| E["return (无提供方)"]
    
    B -->|"否 (有数据)"| F["缓存到 cachedInvokerUrls"]
    F --> G["toInvokers(oldMap, urls)<br/>将 URL 转换为 Invoker"]
    
    G --> H["遍历每个 URL"]
    H --> I{"oldUrlInvokerMap<br/>中是否已存在?"}
    I -->|"是 (URL 未变)"| J["复用已有 Invoker"]
    I -->|"否 (新 URL)"| K["protocol.refer(type, url)<br/>创建新 Invoker"]
    
    J --> L["newUrlInvokerMap"]
    K --> L
    
    L --> M["refreshRouter(finalInvokers)"]
    M --> N["setInvokers(finalInvokers)"]
    N --> O["destroyUnusedInvokers(oldMap, newMap)<br/>销毁不再使用的 Invoker"]
    O --> P["invokersChanged()<br/>通知 Invoker 变更"]
```

核心源码 [`RegistryDirectory.refreshInvoker()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/integration/RegistryDirectory.java#L260-L370)：

```java
private void refreshInvoker(List<URL> invokerUrls) {
    // 空保护：如果收到空列表且有缓存，使用缓存
    if (invokerUrls.isEmpty() && CollectionUtils.isNotEmpty(localCachedInvokerUrls)) {
        invokerUrls.addAll(localCachedInvokerUrls);
    }
    
    // URL → Invoker 转换
    Map<URL, Invoker<T>> newUrlInvokerMap = toInvokers(oldUrlInvokerMap, invokerUrls);
    
    // 构建最终 Invoker 列表
    List<Invoker<T>> newInvokers = new ArrayList<>(newUrlInvokerMap.values());
    BitList<Invoker<T>> finalInvokers = new BitList<>(newInvokers);
    
    // 刷新路由链
    refreshRouter(finalInvokers.clone(), () -> this.setInvokers(finalInvokers));
    
    // 更新 URL→Invoker 映射
    this.urlInvokerMap = newUrlInvokerMap;
    
    // 销毁不再使用的 Invoker
    destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
}
```

---

## 六、FailbackRegistry — 失败重试机制

### 6.1 订阅失败重试流程

```mermaid
flowchart TD
    A["FailbackRegistry.subscribe(url, listener)"] --> B["super.subscribe(url, listener)<br/>AbstractRegistry 记录订阅关系"]
    B --> C["removeFailedSubscribed(url, listener)<br/>清除之前的失败记录"]
    C --> D["doSubscribe(url, listener)<br/>调用子类实现"]
    
    D --> E{订阅成功?}
    E -->|成功| F["完成"]
    E -->|失败| G{check=true<br/>或 skipFailback?}
    
    G -->|是| H["直接抛出异常"]
    G -->|否| I["尝试使用本地缓存"]
    
    I --> J{本地缓存有数据?}
    J -->|是| K["notify(url, listener, cachedUrls)<br/>使用缓存数据通知"]
    J -->|否| L["addFailedSubscribed(url, listener)<br/>加入失败重试队列"]
    
    L --> M["HashedWheelTimer<br/>定时重试"]
    M --> N["FailedSubscribedTask.run()"]
    N --> O["doSubscribe(url, listener)"]
    O --> E
```

### 6.2 失败重试核心源码

[`FailbackRegistry.subscribe()`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/support/FailbackRegistry.java#L345-L385)：

```java
@Override
public void subscribe(URL url, NotifyListener listener) {
    super.subscribe(url, listener);            // 记录订阅关系
    removeFailedSubscribed(url, listener);      // 清除之前的失败记录
    try {
        doSubscribe(url, listener);             // 调用子类实现
    } catch (Exception e) {
        // 尝试使用本地缓存文件
        List<URL> urls = getCacheUrls(url);
        if (CollectionUtils.isNotEmpty(urls)) {
            notify(url, listener, urls);        // 使用缓存通知
        } else {
            // 加入失败重试队列
            addFailedSubscribed(url, listener);
        }
    }
}
```

---

## 七、AbstractRegistry — 本地缓存与通知

### 7.1 本地文件缓存

[`AbstractRegistry`](file:///d:/workspace/java_projects/source_projects/dubbo/dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/support/AbstractRegistry.java) 提供了本地磁盘缓存能力，在注册中心不可用时仍能使用上次缓存的提供方列表：

```
~/.dubbo/dubbo-registry-{appName}-{registryAddress}.cache
```

```java
// AbstractRegistry.notify() 中保存到本地文件
protected void notify(URL url, NotifyListener listener, List<URL> urls) {
    // 1. 保存到内存 notified Map
    notified.computeIfAbsent(url, k -> new ConcurrentHashMap<>())
            .put(url.toFullString(), urls);
    
    // 2. 保存到本地磁盘文件
    saveProperties(url);
    
    // 3. 回调 listener
    listener.notify(urls);
}
```

### 7.2 空保护机制

当注册中心推送空的提供方列表时，`RegistryDirectory` 会使用本地缓存的 URL 列表，避免因注册中心异常导致服务不可用：

```java
// RegistryDirectory.refreshInvoker()
if (invokerUrls.isEmpty() && CollectionUtils.isNotEmpty(localCachedInvokerUrls)) {
    logger.warn("Service " + serviceKey + " received empty address list, trigger empty protection.");
    invokerUrls.addAll(localCachedInvokerUrls);
}
```

---

## 八、提供方变更 → 消费方感知完整链路

```mermaid
sequenceDiagram
    participant P as 提供方
    participant Nacos as Nacos Server
    participant NR as NacosRegistry
    participant FB as FailbackRegistry
    participant AR as AbstractRegistry
    participant RD as RegistryDirectory
    participant Cluster as ClusterInvoker

    Note over P,Nacos: === 提供方上线 ===
    P->>Nacos: registerInstance(serviceName, instance)
    Note over Nacos: 实例注册成功
    
    Note over Nacos,Cluster: === Nacos 推送变更 ===
    Nacos-->>NR: NamingEvent (实例变更通知)
    NR->>NR: EventListener.onEvent()
    NR->>NR: NacosAggregateListener 聚合
    NR->>NR: notifySubscriber(url, serviceName, listener, instances)
    NR->>NR: buildURLs(url, instances)
    NR->>FB: notify(url, listener, urls)
    
    Note over Nacos,Cluster: === 本地缓存 + 通知 ===
    FB->>AR: AbstractRegistry.notify()
    AR->>AR: 保存到本地磁盘缓存文件
    AR->>RD: listener.notify(urls)
    
    Note over Nacos,Cluster: === Directory 刷新 Invoker ===
    RD->>RD: notify(urls)
    RD->>RD: 分类: providers/configurators/routers
    RD->>RD: refreshInvoker(providerURLs)
    RD->>RD: toInvokers(oldMap, urls)
    Note over RD: 为新 URL 创建 Invoker<br/>protocol.refer(type, newUrl)
    RD->>RD: destroyUnusedInvokers()
    RD->>RD: setInvokers(newInvokers)
    
    Note over Cluster: Invoker 列表已更新<br/>下次调用自动包含新提供方
    
    Note over P,Nacos: === 提供方下线 ===
    P->>Nacos: deregisterInstance(serviceName, instance)
    Nacos-->>NR: NamingEvent (实例变更通知)
    NR->>RD: notify(urls_without_offline_instance)
    RD->>RD: refreshInvoker(reducedUrls)
    Note over RD: 销毁下线实例的 Invoker<br/>从列表中移除
```

---

## 九、关键设计特点总结

| 特性 | 实现方式 |
|------|----------|
| **注册中心 SPI** | `RegistryFactory` + `Registry` 接口，`nacos=...NacosRegistryFactory` |
| **服务名格式** | `providers:{interface}:{version}:{group}` |
| **订阅方式** | 先全量拉取 + 后事件监听（长轮询/UDP 推送） |
| **失败重试** | `FailbackRegistry` + `HashedWheelTimer` 定时重试 |
| **本地缓存** | `AbstractRegistry` 磁盘文件缓存，注册中心不可用时降级 |
| **空保护** | `RegistryDirectory` 收到空列表时使用缓存 URL |
| **增量更新** | `toInvokers()` 对比新旧 URL，复用已有 Invoker，销毁无用 Invoker |
| **分类通知** | URL 按 `providers`/`configurators`/`routers` 分类处理 |
| **聚合监听** | `NacosAggregateListener` 将多个 Nacos serviceName 聚合为一个 Dubbo listener |
| **引用计数** | `ReferenceCountExchanger` 共享连接管理 |

---

## 十、核心源码文件索引

| 文件 | 职责 |
|------|------|
| `Registry.java` | Registry 接口定义 |
| `AbstractRegistry.java` | 本地缓存、通知机制 |
| `FailbackRegistry.java` | 失败重试模板 |
| `RegistryProtocol.java` | 编排 refer/export 流程 |
| `DynamicDirectory.java` | Directory 抽象基类 |
| `RegistryDirectory.java` | 订阅通知处理、Invoker 刷新 |
| `NacosRegistryFactory.java` | Nacos Registry 工厂 |
| `NacosRegistry.java` | Nacos 注册中心核心实现 |
| `NacosNamingServiceWrapper.java` | Nacos SDK 封装 |
| `NacosAggregateListener.java` | 聚合监听器 |
| `NacosServiceName.java` | Nacos 服务名构建 |
| `NotifyListener.java` | 通知监听器接口 |
| `RegistryNotifier.java` | 通知执行器 |
