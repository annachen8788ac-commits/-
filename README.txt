Signal 群实时号码数据库部署包

历史数据：58,511 条，全部保留，包括重复号码。

查询逻辑：
- 查询范围始终只针对号码最后四位。
- 输入第 1 位开始实时显示，第 2、3、4 位继续自动缩小结果。
- 删除数字会立即重新匹配；全部删除后结果清空。
- 查询结果显示：年月日、发送者、完整号码、来源。
- 历史数据与 Signal 新数据一起查询。

数据规则：
- 历史记录不删除、不按号码去重。
- Signal 每一条新消息只追加。
- 只有同一条 Signal 网络消息因重连重复投递时，才用 source_key 防止重复写入。
- 同一个号码不同日期、不同人、不同消息出现，全部分别保存。

Railway 建议服务：
1. PostgreSQL
2. signal-cli-rest-api
3. tracker（本部署包）

环境变量：
DATABASE_URL
SIGNAL_API_URL
SIGNAL_NUMBER
SIGNAL_GROUP_ID
APP_TIMEZONE=America/Los_Angeles
