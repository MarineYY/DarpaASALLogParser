# DrapaASALLogParser

## 2024.2.28 根据TimeStamp划分数据集

### 1 配置 __Kafka__ 的 __IP__ 地址 
**line-21**
```java
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
```
### 2 配置数据集的路径
**line-25-32**
```java
String folderPathTHEIA = "xxx_fileforder/";
for (int i = 0; i < 3; i ++) {
    ...
    File file = new File(folderPathTHEIA + "ta1-trace-e3-official-1.json." + i);
    if (i == 0) file = new File(folderPathTHEIA + "ta1-trace-e3-official-1.json");
    ...
}
```
具体路径和文件名称根据具体情况调整

### 3 配置 **Topic** 前缀xxx，划分后的训练集Topic为 xxx-train，测试集Topic为 xxx-test
**line-31**
```java
sendLog(file, properties, "xxx");
```

### 4 配置 **TimeStamp**
**line-70**
```java
if(log.getEventData().getEHeader().getTs() < 1523633125720000000L){
```

### 5 运行对应的数据序列化器

## 2024.3.16 重构数据集解析

### 1 使用步骤

[如上](#1-配置-__kafka__-的-__ip__-地址-)
### 2 相关问题记录

- `Trace`和`FiveDirections`数据集中的`srcSinkObject`和`unNamedPipeObject`实体都没有处理
- `OnlineGraphAlignment`接收端发现部分事件的`uuid`相同（因为`uuid`的构造和`subject`的`ppid`有关，而`ppid`在缺失的时候都赋值为0）
- `Theia`数据集中没有记录`RECVMSG`和`SENDMSG`事件，只记录了`RECVFROM`和`SENDTO`
- `FIVEDIRECTIONS`数据集中`subject`实体没有`name`属性，用的是`cmdline`来赋值，`pid`用的是`cid`来赋值（`cid`每条记录都有）
- `FIVEDIRECTIONS`数据集中的`IP`地址有`ipv6`格式的地址，且存在`ipv6`中的多播地址，目前的处理方法是设置为`-1.-1.-1.-1`

### 3 相关文档

`src/main/Documents/examples/`下有三个数据集里典型事件的示例，每一个文件的内容都是：源节点 - 事件 - 汇节点
`src/main/Documents/darpa数据集信息.xlsx`里有数据集的统计信息
`src/main/Documents/darpa数据处理文档.xlsx`更详细地说明了一些处理逻辑和遗留问题