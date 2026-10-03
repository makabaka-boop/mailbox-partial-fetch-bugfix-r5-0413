
UID FETCH 支持 BODY[]、BODY.PEEK[] 与 <offset.count> 字节片段，多个 body 项按请求顺序返回。BODY[] 标记 Seen，PEEK 不改变标记；标记变化按邮箱修订次序通知其余已选择会话。超出正文末尾返回零字节；非法字段或范围不得改变存储。
