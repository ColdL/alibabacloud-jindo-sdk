# 使用 PyFlink 写入 JindoFS 的实践

---

## 环境要求

在集群上有开源版本 Flink 软件，版本不低于 1.10.1。

完成 JindoFS SDK 的配置，配置方式参考 [Flink](/docs/flink/jindofs_sdk_on_flink_for_jfs.md) 中 “SDK 配置” 一节。

## 使用方法

配置好 SDK 后，无需额外配置，以常规 PyFlink 作业方式使用即可，注意打开 Checkpoint 并使用正确的路径。写入 JindoFS 须以 jfs:// 为前缀。

JindoFS 介绍和使用参见文档首页 [关于 JindoFS](/README.md#关于-jindofs)

## 以 HDFS 文件为数据源，写入 JindoFS 的示例程序

* 本示例程序基于 Flink-1.11，其他 Flink 版本可能略有出入。

#### 放置数据文件于 HDFS

首先考虑选择一个本地临时路径，记作 &lt;local-tmp&gt;，然后在临时路径下用下列命令生成一个文本文件：
```
echo -e "This\nis\na\ndemo" > <local-tmp>
```

然后将该文件放置于 HDFS，假设 HDFS 路径为 &lt;hdfs-path&gt;，那么用如下命令即可：
```
hadoop fs -copyFromLocal <local-tmp> <hdfs-path>
```

#### 以 HDFS 为数据源，写入 JindoFS 的 PyFlink 程序

写入 JindoFS 的路径须以 jfs:// 为前缀，记该路径为 &lt;jfs-path&gt;，请确保该路径目前并不存在，Flink 才可正常创建该目录并写入数据。

下面是该示例程序，其中 &lt;jfs-path&gt; 为写入 JindoFS 的路径，&lt;hdfs-path&gt; 为上一步骤 HDFS 数据源文件的路径：
```
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def sink_demo():
    checkpoint_interval = 5000

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(checkpoint_interval, CheckpointingMode.EXACTLY_ONCE)
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

    source_ddl = """
        CREATE TABLE mySource (
            word VARCHAR
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '<hdfs-path>'
        )
    """

    sink_ddl = """
        CREATE TABLE mySink (
            word VARCHAR,
            ct BIGINT
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '<jfs-path>'
        )
    """

    t_env.sql_update(source_ddl)
    t_env.sql_update(sink_ddl)

    t_env.from_path('mySource').select('word, 1').insert_into('mySink')
    t_env.execute('demo')


sink_demo()
```

这段程序开启了 Flink 的 Checkpoint 功能，间隔为 5 秒（5000 毫秒）。您可以直接在文本编辑器里复制粘贴上述代码，然后将 &lt;jfs-path&gt; 与 &lt;hdfs-path&gt; 替换为实际的路径，也可以在 IDE 里完成这些工作。如果希望在 IDE 里对上述代码进行语法检查，请确保配置好 Python 解释器，并安装 apache-flink 软件包。该源代码文件命名为 demo.py，并放置于集群当前目录下。

#### 提交程序运行

在 Flink 集群上，假设 Flink 的安装路径为 &lt;FLINK_HOME&gt;，那么可以用下列程序提交运行：
```
<FLINK_HOME>/bin/flink run -m <mode> -py demo.py
```

其中，&lt;mode&gt; 为运行模式，例如 yarn-cluster 等。Flink 提交 python 程序运行的参数可通过 --help 查看，更多信息请参考 Flink 开源文档。

#### 检查结果

程序应能够很快运行结束，检查 &lt;jfs-path&gt; 路径应为非空，有至少一个文件。将文件拼接后，结果应为：
```
This 1
is 1
a 1
demo 1
```

## 以 Kafka 为数据源，写入 JindoFS 的示例程序

#### 部署 Kafka 集群

Kafka 集群的部署和基本使用请参考 Kafka 官方文档，此处不再赘述。

#### 配置 Kafka connector

以 Kafka 作为数据源时，为了 Flink 能够读取 Kafka 的数据，需要额外配置 Kafka connector。这是 Flink-1.11 下载和配置 Kafka connector 的 [文档](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html)，其他 Flink 版本请参考官方文档。

#### Python 安装 kafka 扩展包

为了能够提交 Python 版本的 Kafka 数据生成程序，请提前安装 kafka 扩展包，安装方式：
```
pip3 install kafka
```

推荐在虚拟 Python 环境下安装扩展并提交，方式请参考 Python 使用文档。

#### Kafka 数据生成程序

约定 Kafka 主题 (topic) 为 myTest，首先在 Kafka 集群创建出该主题。

下面是一个向该主题发送数据的程序，每隔 0.1 秒发送一批，每批发送 1000 条数据，一共持续大约 6 分钟。其中 &lt;kafka-server&gt; 根据 Kafka 集群环境配置，程序如下：
```
import math
import time
import random
from json import dumps
from kafka import KafkaProducer


def batch_send():
    server = "<kafka-server>"
    topic = "myTest"
    count = 3000000
    interval = 0.1
    limit = 100
    batch = 1000
    producer = KafkaProducer(bootstrap_servers=[server], value_serializer=lambda x: dumps(x).encode('utf-8'))

    loop = int(math.ceil(float(count) / float(batch)))
    remaining = count
    for i in range(0, loop):
        num = batch
        if remaining < num:
            num = remaining
        for j in range(0, num):
            data = random.randint(0, limit)
            producer.send(topic, value={"uid": data})
        remaining -= num
        time.sleep(interval)


if __name__ == '__main__':
    batch_send()
```

将该程序保存为文本格式文件 producer.py 并放置于 Kafka 集群，待 PyFlink 程序提交后再运行产生数据。

#### 以 Kafka 为数据源，写入 JindoFS 的 PyFlink 程序

与以 HDFS 为数据源的程序类似，只不过 Source 替换为 Kafka。为了演示更多功能，还增加了用户定义程序 (UDF) 与动态分区写入，如不需要可适当精简。

程序中 &lt;kafka-server&gt; 根据 Kafka 集群环境配置，代码如下：
```
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, CheckpointingMode
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import udf

PID_LIMIT = 3


@udf(input_types=[DataTypes.INT()], result_type=DataTypes.INT())
def uid_to_pid(uid):
    return uid % PID_LIMIT


def demo():
    kafka_server = "<kafka-server>"
    kafka_topic = "myTest"
    sink_dest = "<jfs-path>"
    checkpoint_interval = 5000

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(checkpoint_interval, CheckpointingMode.EXACTLY_ONCE)
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

    source_ddl = f"""
        CREATE TEMPORARY TABLE mySource (
            uid INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{kafka_server}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """

    sink_ddl = f"""
        CREATE TABLE mySink (
            uid INT,
            pid INT
        ) PARTITIONED BY (
            pid
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{sink_dest}',
            'format' = 'csv',
            'sink.rolling-policy.file-size' = '2MB',
            'sink.partition-commit.policy.kind' = 'success-file'
        )
    """

    t_env.sql_update(source_ddl)
    t_env.sql_update(sink_ddl)
    t_env.register_function('uid_to_pid', uid_to_pid)

    query = "SELECT uid, uid_to_pid(uid) FROM mySource"
    t_env.sql_query(query).insert_into("mySink")

    t_env.execute("demo")


if __name__ == '__main__':
    demo()

```

* 特别提示：数据只有在 Checkpoint 完成阶段才会写入文件系统，所以请务必打开 Checkpoint 功能。

请将该代码以文本文件格式保存在 Flink 集群，名称为 demo.py，并将 &lt;kafka-server&gt; 与 &lt;jfs-path&gt; 根据实际情况替换。

#### 提交程序运行

首先在 Flink 集群，以下列命令提交程序，部分参数参考之前 “以 HDFS 为数据源” 的说明：
```
<FLINK_HOME>/bin/flink run -m <mode> -py demo.py -j <kafka-connector>.jar
```

其中，&lt;kafka-connector&gt; 是从官方下载获取的合适版本的 Kafka connector，参见 “配置 Kafka connector” 部分。

然后在 Kafka 集群，以下列方式提交，向 Kafka 发送数据：
```
python3 producer.py
```

此时 Flink 应当能够自 Kafka 接收数据，并写入 JindoFS。

#### 检查结果

等待约 6 分钟后，Kafka 集群的数据生产程序 producer.py 应该发送了所有数据，Python 进程自动停止。此时 Flink 集群仍在等待更多数据，可以手动停止 Flink 作业。如果作业以 yarn-cluster 模式运行，需要在 yarn 上停止作业。

检查 &lt;jfs-path&gt; 路径应为非空，且有多个分区目录存在，文件内容应类似于：
```
35
14
74
```

即一列数字。
