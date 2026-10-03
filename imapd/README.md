# imapd — 限定邮箱协议的 Go TCP 服务

一个简化 IMAP 风格的 TCP 服务：带标签命令、按声明字节数读取的消息体、SQLite
持久化、递增 UID、多连接按邮箱修订顺序接收变更通知。

## 运行

```sh
go build ./cmd/imapd
./imapd -addr 127.0.0.1:1143 -db imapd.db
go test -race ./...        # 两个真实 TCP 客户端的交错测试
```

## 协议

每行一个命令：`tag COMMAND args...\r\n`。响应为任意数量的非标签行
（`* ...`）后跟一个标签行（`tag OK/NO/BAD ...`）。

| 命令 | 说明 |
|---|---|
| `tag SELECT box` | 选中邮箱；返回 `* n EXISTS`、`* OK [UIDNEXT u]` |
| `tag APPEND box [(flags)] {n}\r\n<n 字节>\r\n` | 追加消息；按声明字节数精确读取消息体 |
| `tag UID FETCH set (FLAGS)` / `(BODY[])` / `(BODY.PEEK[])` | 按 UID 查询；每个数据项可带 `<offset.count>` 字节片段，同一帧可含任意多个 body 项，按请求顺序返回；`BODY[]` 标记 Seen，`BODY.PEEK[]` 不改标记；返回 `* seq FETCH (UID u FLAGS (...) [BODY[] {n}...])` |
| `tag UID STORE set [+-]FLAGS[.SILENT] (flags)` | 替换/增加/移除标志；广播 `* seq FETCH (UID u FLAGS (...))` |
| `tag EXPUNGE` | 删除所有 `\Deleted` 消息；每条广播 `* seq EXPUNGE` |
| `tag NOOP` / `tag LOGOUT` | 空操作 / 断开（先 `* BYE`） |

UID 集合语法：`1,3,5:7,*`（`*` = 当前最大 UID，区间含端点、可逆序）。

## 关键语义与实现

- **消息体即载荷**：`{n}` 之后用 `io.ReadFull` 精确读 n 字节。体内的换行、
  伪命令、`{...}` 文本一律不解释。命令可以分任意多段到达（`bufio` 重组），
  也可以多条流水线连发。
- **中途断开不留半条消息**：只有完整读完字面量及其结尾 CRLF 后才执行
  SQLite 事务；连接中途断开则什么都不写入。多行删除（EXPUNGE）同样在
  单个事务里。
- **UID 永不复用**：UID 取自 `mailboxes.next_uid` 计数器（单调递增、持久化，
  重启后仍然有效）。**序号不存储**，始终按 `ORDER BY uid` 的位置现算，因此
  删除后自然重排，而 UID 身份不变——所有寻址命令都是 UID 的，重排后的序号
  永远不会被错当成旧消息。
- **预览与已读分离**：`UID FETCH` 逐项解析请求项。片段 `<offset.count>` 始终按
  原始字节（octet）解释，中文多字节字符与二进制内容不发生偏移错位；字面量
  声明的字节数恒等于实际发送字节数。越过正文末尾返回零字节，count 越过末尾则
  截断到剩余长度（RFC 3501 的 `BODY[]<offset>` 回显形式）。一条命令里的多个
  body 项合入同一 FETCH 帧并按请求顺序返回；多封邮件按序号顺序各发一帧。
  只有非 PEEK 的 `BODY[]` 会把 `\Seen` 置上——`BODY.PEEK[]` 预览保持原标记；
  同一命令对一封邮件至多产生一次 Seen 变化，其他选中会话按邮箱修订次序收到
  恰好一条 FLAGS 通知。
- **FETCH/STORE 的原子标记写入**：整条命令先校验参数（非法项/范围一律 `BAD`，
  存储不变），再在单个 SQLite 事务里写完全部标记；写入中途失败则整体回滚、
  回 `NO storage error`，既不返回任何 FETCH 数据也不广播。提交成功后才在同一
  把邮箱锁内入队发起者响应与他端通知，因此响应内容、UID/序号、持久化标记和
  两端的变化顺序都对应同一次有效操作。
- **修订顺序通知**：每个邮箱一个 `mailboxHub` 互斥锁。任何变更都在持锁期间
  落库并生成通知帧，经各会话的 FIFO 出站队列广播给所有选中该邮箱的会话
  （含发起者；`.SILENT` 除外）。因此每个连接都按邮箱修订顺序收到
  `* n EXISTS` / `* seq FETCH` / `* seq EXPUNGE`。EXPUNGE 逐条生成事件，
  序号取该次删除瞬间的值（先删的先报，后续按已重排的序号）。
- **限制**：单封 16 KB（超限字面量会被读完并丢弃再回 `NO`，连接保持同步），
  每箱 200 封（`NO mailbox full`）。
- **单写者连接**：每个会话一个写协程独占 socket 写，命令响应与异步通知都
  经过同一队列，保证客户端看到的顺序。

## 测试

`server_test.go` 中的 `TestTwoClientsInterleaved` 用两个真实 TCP 客户端
（A、B）选中同一邮箱并交错操作：流水线/分片 APPEND、标志增删替换、
`.SILENT`、单条与多条 EXPUNGE、交错各 5 封追加、断连于字面量中途、
16 KB 超限、填满 200 封。全程逐行断言两个客户端收到的通知顺序、
UID 查询结果（含重排后 `UID FETCH/STORE 2` 必须落空）、正文逐字节一致、
以及 UID 不复用（最终 `UIDNEXT 204`）。
