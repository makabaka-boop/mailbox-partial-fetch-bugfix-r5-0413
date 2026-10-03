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
| `tag UID FETCH set (item...)` | item ∈ `FLAGS`、`BODY[]`、`BODY.PEEK[]`，body 项可带 `<offset.count>` 字节片段；返回 `* seq FETCH (UID u FLAGS (...) [BODY[][<o>] {n}...])`，多个 body 项按请求顺序返回 |
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
- **修订顺序通知**：每个邮箱一个 `mailboxHub` 互斥锁。任何变更都在持锁期间
  落库并生成通知帧，经各会话的 FIFO 出站队列广播给所有选中该邮箱的会话
  （含发起者；`.SILENT` 除外）。因此每个连接都按邮箱修订顺序收到
  `* n EXISTS` / `* seq FETCH` / `* seq EXPUNGE`。EXPUNGE 逐条生成事件，
  序号取该次删除瞬间的值（先删的先报，后续按已重排的序号）。
- **取正文与已读状态**：`BODY[]` 把取到的邮件标记 `\Seen`，`BODY.PEEK[]`
  不改变标记；`<o.c>` 按原始字节切片——多字节字符与二进制内容都按字节计，
  起点越过正文末尾返回零字节、超出部分截断。整个 FETCH 的标记变化是一个
  修订：先在单个事务里落库，发起者的 FETCH 响应里即带新标记，其余已选中
  会话按修订次序各收一条 `* seq FETCH (UID u FLAGS (...))`；已带 `\Seen`
  的邮件不再产生通知。非法字段或范围回 `BAD`，存储不变。UID STORE 的
  多封标记更新同样在单个事务里，中途失败不留写了一半的标记。
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

`TestFetchBodyRangesAndSeenSync` 覆盖中文加二进制正文的字节级片段
（含切断多字节字符的范围）、PEEK 不改标记、`BODY[]` 恰好一次置 `\Seen`
并同步给另一会话、同一命令多个范围按序返回、越界返回零字节与截断、
非法字段/范围回 `BAD` 且不动存储、EXPUNGE 重排后两端序号一致。
`TestFetchFlagFailureIsAtomic` 用故障注入让多封邮件的标记写入中途失败，
验证 FETCH 与 STORE 都整体回滚：无半截标记、两端无半截通知。
