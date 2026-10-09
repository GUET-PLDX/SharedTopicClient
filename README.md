# SharedTopicClient

SharedTopicClient 是一个多 Topic 数据共享与串口转发客户端模块。它用于通过 UART 将多个 Topic 的数据统一打包、发送，实现消息流的串口透明同步转发，适用于分布式系统的多主题数据同步或边缘数据采集。

SharedTopicClient is a client module for multi-topic data sharing and transparent UART forwarding. It subscribes to multiple Topics, packs their updates, and transmits them via UART, enabling efficient and reliable message synchronization over serial connections—ideal for distributed systems or edge data acquisition.

---

## 硬件需求 / Required Hardware

- uart_name

## 构造参数 / Constructor Arguments

- uart_name: 串口设备名 / UART device name (e.g., "uart_cdc")
- slot_count: 共享待发槽位数量。每个槽位的字节数由订阅 Topic 中最大的打包后长度自动计算。
  / Number of shared pending slots. Each slot size is derived from the largest packed subscribed Topic.
- topic_configs: 需要订阅并转发的 Topic 配置列表。每项可以只写 topic 名，也可以写
  `[topic, domain]`。/ Topic configs to subscribe and forward. Each item may be a
  topic name or `[topic, domain]`.

## 运行方式

`SharedTopicClient` 不创建发送线程。模块注册 Topic callback；每次 Topic 发布时，
callback 先从空槽位队列申请一个 packet 槽，完成打包后把 `{槽位, 长度}` 放入待发
pool，然后尝试交给 UART `WritePort`：

- 所有 Topic 共用同一组固定 packet 槽位。
- 空槽位和待发 packet 都使用 `MPMCQueue`；发送完成或提交失败后归还槽位。
- 空闲队列额外预留一个单元，避免 Topic 消费者在 Pop 发布完成前被发送完成回调抢占时，归还槽位误报 FULL。
- Topic 回调并发或重入时丢弃新更新；空槽申请失败时也丢弃新更新。
- 发送门控保证最多一个未完成写入，并将同步完成回调转换为循环推进，避免逐包递归。
- UART 提交失败（包括 BUSY/FULL）时丢弃当前包并继续处理待发队列，不重试。
- UART 应由本模块独占；本模块不提供可靠传输、重传或对端确认。
- 对象与槽位按 LibXR 初始化期分配模型存活至系统结束，不能在注册回调后销毁。

实现集中在 `SharedTopicClient.hpp`，构造参数、Topic 数据和 timestamp 协议保持不变。

## Timestamp

`SharedTopicClient` 转发 Topic 时会保留 libxr message envelope timestamp：

1. 本地 Topic callback 收到 `(timestamp, payload)`。
2. `Topic::PackData(topic_crc, buffer, timestamp, payload)` 写入串口包。
3. 对端 `SharedTopic` 解析后用同一个 timestamp 发布到对端 domain。

因此同步类 topic 不需要在 payload 里重复携带时间戳；payload 只保留业务字段即可。

## 依赖 / Depends

- 无（No dependencies）
