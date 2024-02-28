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
