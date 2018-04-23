# sparksql-stats
基于PySpark库，使用SparkSql连接MYSQL数据库并对数据进行统计分析的基础架构。

**注：下面的配置是基于已经安装好Spark单点或集群的情况下所写。**

## 版本控制
1. Spark版本为2.3
2. Python版本为2.7
3. Mysql版本为5.6

## 更换ubuntu 16的源
```bash
$ sudo cp /etc/apt/sources.list /etc/apt/sources.list.bak
$ sudo vim /etc/apt/sources.list
""
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial main restricted
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-updates main restricted
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial universe
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-updates universe
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial multiverse
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-updates multiverse
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-backports main restricted universe multiverse
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-security main restricted
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-security universe
deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-security multiverse
""

$ sudo apt-get update 
$ sudo apt-get update --fix-missing
```

## 安装依赖
```bash
$ sudo apt-get install -y tar wget git
$ sudo apt-get install -y openjdk-8-jdk

$ sudo apt-get -y install build-essential python-dev python-pip python-six python-virtualenv libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev zlib1g-dev
```

## 安装 Mysql Connector
1. 打开Mysql JDBC驱动的地址https://dev.mysql.com/downloads/connector/j/, 选择Platform Independent系统，找到对应的版本下载mysql connector。
2. 解压缩数据包后可以找到文件 mysql-connector-java-8.0.11.jar
3. 复制文件到spark/jars文件夹下面
```bash
$ cp mysql-connector-java-8.0.11/mysql-connector-java-8.0.11.jar /opt/spark/jars/
```
### 配置 Spark Jars
```bash
$ cd /opt/spark
$ cp conf/spark-defaults.conf.template conf/spark-defaults.conf
$ vim conf/spark-defaults.conf
""
spark.jars  /opt/spark/jars/mysql-connector-java-8.0.11.jar
""
```

### 配置 Spark
下面的配置数据是SPARK单机模式的简单配置范例
```bash
$ cp conf/spark-env.sh.template conf/spark-env.sh
$ vim conf/spark-env.sh
""
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

export SPARK_HOME=/opt/spark
export SPARK_LOCAL_IP=127.0.0.1
""
```

### 测试Connector安装完毕
进入pyspark模式
```bash
$ ./bin/pyspark
```

测试
```python
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)
mysql_table = sqlContext.read.format("jdbc").options(
    url="jdbc:mysql://192.168.33.11:3306/ccnu_resource", 
    dbtable="paper", 
    user="zhexiao", 
    password="password").load()
mysql_table.printSchema()
```

## 安装pyspark库
创建虚拟环境
```bash
$ virtualenv env
$ source env/bin/activate
```

```bash
$ cd spark-2.3.0-bin-hadoop2.7/python
$ python setup.py sdist
$ pip install dist/*.tar.gz 或者  sudo -H pip install dist/*.tar.gz
```

## 项目配置
1. 复制**settings.py.example**为**settings.py**并按需配置数据。

## 运行测试文件
```bash
$ cd sparksql-stats
$ python spark_test.py
```
### 输出结果
```bash
[u'{"cognition_map_num":"MJ14-2-2-3","count":7}', u'{"cognition_map_num":"MJ14-2-2-4","count":5}', u'{"cognition_map_num":"MJ14-4","count":3}', u'{"cognition_map_num":"MJ14-3-1","count":3}', u'{"cognition_map_num":"MJ14-2-2","count":3}', u'{"cognition_map_num":"MJ14-3-3","count":2}', u'{"cognition_map_num":"MJ14-2-2-2","count":2}', u'{"cognition_map_num":"MJ14-3-2","count":2}', u'{"cognition_map_num":"MJ14-1-4","count":1}', u'{"cognition_map_num":"MJ14-3-3-1","count":1}', u'{"cognition_map_num":"MJ14-2","count":1}']
[u'{"diff":85,"count":20}', u'{"diff":65,"count":35}', u'{"diff":53,"count":12}', u'{"diff":76,"count":9}', u'{"diff":47,"count":9}', u'{"diff":52,"count":49}', u'{"diff":40,"count":135}', u'{"diff":57,"count":68}', u'{"diff":54,"count":3}', u'{"diff":48,"count":1}', u'{"diff":64,"count":5}', u'{"diff":61,"count":1}', u'{"diff":72,"count":6}', u'{"diff":55,"count":101}', u'{"diff":100,"count":1}', u'{"diff":51,"count":1}', u'{"diff":63,"count":43}', u'{"diff":10,"count":1}', u'{"diff":77,"count":1}', u'{"diff":50,"count":2204}', u'{"diff":45,"count":11}', u'{"diff":82,"count":1}', u'{"diff":80,"count":292}', u'{"diff":73,"count":19}', u'{"diff":70,"count":3047}', u'{"diff":62,"count":25}', u'{"diff":60,"count":4024}', u'{"diff":90,"count":15}', u'{"diff":75,"count":2}', u'{"diff":56,"count":42}', u'{"diff":58,"count":6}', u'{"diff":11,"count":3}', u'{"diff":68,"count":3}', u'{"diff":99,"count":1}', u'{"diff":30,"count":2}', u'{"diff":66,"count":45}', u'{"diff":67,"count":8}', u'{"diff":46,"count":1}', u'{"diff":0,"count":1}']
[u'{"question_id":"ysrjhankdg3p1zt1l106","count":12}', u'{"question_id":"ysrjhankdg3p1zt1l105","count":11}', u'{"question_id":"ysrjhankdg3p1zt1l101","count":11}', u'{"question_id":"ysrjhankdg3p1zt1l104","count":10}', u'{"question_id":"ysrjhankdg3p1zt1l102","count":10}', u'{"question_id":"ysrjhankdg3p1zt4l115","count":10}', u'{"question_id":"ysrjhankdg3p1zt1l103","count":10}', u'{"question_id":"ys3ngk2nmnp1zt0416","count":9}', u'{"question_id":"ys3ngk2nmnp1zt0417","count":8}', u'{"question_id":"ysrjhankdg3p1zt4l105","count":6}', u'{"question_id":"ysrjhankdg3p1zt1l108","count":6}', u'{"question_id":"ys3ngk2nmnp1zt0518","count":5}', u'{"question_id":"ysrjhankqxzhg3p0104","count":5}', u'{"question_id":"ysrjhankdg3p1zt4l202","count":5}', u'{"question_id":"ysrjhankdg3p1zt1l111","count":5}', u'{"question_id":"ysrjhankdg3p1zt1l107","count":5}', u'{"question_id":"ys3ngk2nmnp1zt0514","count":5}', u'{"question_id":"ysrjhankdg3p1zt4l118","count":4}', u'{"question_id":"ys3ngk2nmnp1zt0508","count":3}', u'{"question_id":"ysrjhankdg3p1zt4l209","count":3}']
[u'{"cognition_map_num":"YS2-1","count":930}', u'{"cognition_map_num":"YS2-2-1-2","count":622}', u'{"cognition_map_num":"YS4-7","count":490}', u'{"cognition_map_num":"YS8","count":433}', u'{"cognition_map_num":"YS4-5","count":356}', u'{"cognition_map_num":"YS4-4","count":346}', u'{"cognition_map_num":"YS4-1","count":331}', u'{"cognition_map_num":"YS1-4","count":325}', u'{"cognition_map_num":"YS3-4-1","count":295}', u'{"cognition_map_num":"YS1-11-1-1","count":276}', u'{"cognition_map_num":"YS8-2-2","count":269}', u'{"cognition_map_num":"YS1-12-1","count":233}', u'{"cognition_map_num":"YS2-2","count":220}', u'{"cognition_map_num":"YS1-10","count":200}', u'{"cognition_map_num":"YS4-6","count":197}', u'{"cognition_map_num":"YS3-3-2","count":193}', u'{"cognition_map_num":"YS7-1-3","count":183}', u'{"cognition_map_num":"YS1-5-3-1","count":179}', u'{"cognition_map_num":"YS5-1-1-2","count":167}', u'{"cognition_map_num":"YS1-5","count":154}']
[u'{"cognition_map_num":"YS1-4-1","count":65}', u'{"cognition_map_num":"YS1-12-1","count":52}', u'{"cognition_map_num":"YS1-4-2","count":45}', u'{"cognition_map_num":"YS1-11-1-3","count":28}', u'{"cognition_map_num":"YS1-10","count":23}', u'{"cognition_map_num":"YS1-8-3","count":15}', u'{"cognition_map_num":"YS1-8-2","count":13}', u'{"cognition_map_num":"YS1-14-1","count":13}', u'{"cognition_map_num":"YS1-5-3-1","count":13}', u'{"cognition_map_num":"YS1-5-4-1","count":12}', u'{"cognition_map_num":"YS1-14-2","count":12}', u'{"cognition_map_num":"YS1-4","count":11}', u'{"cognition_map_num":"YS1-8-4","count":10}', u'{"cognition_map_num":"YS1-8-5","count":9}', u'{"cognition_map_num":"YS5-2-2-2-2","count":8}', u'{"cognition_map_num":"YS1-5-2","count":7}', u'{"cognition_map_num":"YS1-11-1-1","count":7}', u'{"cognition_map_num":"YS1-9-2-2","count":7}', u'{"cognition_map_num":"YS1-9-2-1","count":7}', u'{"cognition_map_num":"YS1-5-6","count":7}']
19.5992779732
关闭spark链接
关闭spark链接
关闭spark链接
```
