## lineage-hive-hook

参考项目[metadata-hive-hook](https://github.com/Bill-cc/metadata-hive-hook.git)

### 介绍
自定义Hive Hook（钩子函数），继承自```org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext```，
通过拦截Hive执行引擎执行后返回给用户前的调用，获取任务执行的相关信息，并且分析用户执行的SQL语句，
获取元数据信息和数据血缘关系，支持字段级别的血缘关系。信息收集后会发送至Kafka消息集群。

### 部署

* 打包

```
cd lineage-hooks
mvn clean package -am -pl lineage-hive-hook -T 8C -Dmaven.compile.fork=true -DskipTests

打包后 jar 包路径
lineage-hooks/lineage-hive-hook/target/lineage-hive-hook-0.0.1-SNAPSHOT.jar
```

* Hook jar 上传到 Hive 集群

```
将编译后的jar包放到集群中hive server所在机器的${HIVE_HOME}/auxlib目录下，

例如：“/opt/cloudera/parcels/CDH/lib/hive/auxlib”

如果auxlib目录不存在，在${HIVE_HOME}下新建auxlib目录。
```

* Hive 配置

```
hive-site.xml中添加hook模块

<property>
    <name>hive.exec.post.hooks</name>
    <value>org.isaac.lineage.hook.hive.LineageHook</value>
</property>
```
* 血缘发送配置

> hook.properties

```
# platformName 平台名称
# clusterName 集群名称
# 这两个配置主要是用来给集群再分个类，
# 环境下可能有多个集群，不同平台使用的集群不一样，都发往血缘平台时可做区分
clusterName=DEFAULT
platformName=DEFAULT
```

> kafka.properties

可以将此配置文件放到和jar包同级目录中，即上面的${HIVE_HOME}/auxlib，会优先加载同级外部的配置文件。

```
# 配置 hive 血缘信息发往的 kafka 信息

#########  Kafka Configs  #########
bootstrap.servers=${YOUR_KAFKA_BOOTSTRAP_IP}:9092
auto.commit.interval.ms=100
auto.commit.enable=false
enable.auto.commit=false
session.timeout.ms=30000
```

