**原文的意思应该是可以有多个进程来处理客户端请求（读），但只有单个进程来处理写入，即一个写者，多个读者**

1. 一个 Bitcask 是一个目录
2. 在一个给定的时间内，只有一个操作系统进程能对这个目录进行写入，这个正在写入的进程可被认为是 db server
3. 任意时刻，该目录中都有一个被 db server 用来写入的 active file
4. 当 active file 超过 size 后，该 file 被 closed，并创建一个 new file 作为 active file
5. 一旦 file 被 close，不管是有意还是无意（断电或崩溃），该 file 都将是 immutable，永远不能再次 open for write
6. active file 只能 append 写
7. active file 中的每个 entry（k/v）的格式为 **crc-tstamp-ksz-valuesz-k-v**（实际中没有横杠作为分隔，都是紧挨着的）
8. kv 被当作 entry 写入落盘后，内存中的 index 被更新，这个 index 是个 hash index，名为 keydir。hash table 的 k 就是
    k-v 的 k，hash table 的 value 是 **fileid-valuesz-valuepos-tstamp**
9. 当 merge 时，所有的 older data file 被 merge 为 merged data file，保存 live or latest 的 k-v entry。
    然后创建一个 hint file，与 data file 格式不同，tstamp-ksz-valuesz-valuepos-k

基于上述模型启动流程：
1. 判断是否有 hint file，如果没有，扫描所有的 data file 来创建 keydir
2. 如果有，只需要扫描 hint file 和 active file 来创建 keydir

基于上述模型的增删改查：
* 增/写入：首先按照 entry 的格式写入 active file，生成 crc。然后更新 keydir。单线程顺序写。
* 删    ：相当于增加，在 active file 写入一个 tomb。删除 keydir 对应的索引。
* 改    ：删旧的+增加新的
* 查    ：先查询 keydir 拿到 fileid，然后去 file 里面根据 valuepos 和 valuesz 进行查。记得校验 crc

merge 进行的时机：
1. older file 总大小超过设置的阈值
2. 定期策略
3. 通过暴露的接口外界手动触发
4. 关闭时
