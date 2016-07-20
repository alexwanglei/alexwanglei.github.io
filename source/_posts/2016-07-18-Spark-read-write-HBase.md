---
title: Spark读写HBase
date: 2016-07-18 22:42:07
categories: Spark
tags: [Spark, HBase]
---
本文介绍使用Spark将RDD数据写入HBase表，以及从HBase表中读取数据创建RDD。
<!-- more -->
## RDD写入HBase
Spark RDD提供了两个API函数**saveAsHadoopDataset**和**saveAsNewAPIHadoopDataset**，将RDD输出到Hadoop支持的存储系统中。
```scala
def saveAsHadoopDataset(conf: JobConf): Unit
	Output the RDD to any Hadoop-supported storage system, 
	using a Hadoop JobConf object for that storage system. 
	The JobConf should set an OutputFormat and any output 
	paths required (e.g. a table name to write to) in the 
	same way as it would be configured for a Hadoop MapReduce job.

def saveAsNewAPIHadoopDataset(conf: Configuration): Unit
	Output the RDD to any Hadoop-supported storage system with new Hadoop API, 
	using a Hadoop Configuration object for that storage system. 
	The Conf should set an OutputFormat and any output paths required 
	(e.g. a table name to write to) in the same way as it would be 
	configured for a Hadoop MapReduce job.
```
两个函数作用相同，只是后者使用了新的Hadoop API。

### 一、使用savaAsHadoopDataset
```scala
object SaveData {
    def main(args: Array[String]): Unit = {
        if(args.length < 1) {
            System.err.println("Usage: SaveData <input file>")
            System.exit(1)
        }
        val sconf = new SparkConf().setAppName("SaveRDDToHBase")
        val sc = new SparkContext(sconf)
        //载入RDD
        val input = sc.objectFile[((String,Int),Seq[(String,Int)])](args(0))

        //创建HBase配置
        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop04")

        //创建JobConf，设置输出格式和表名
        val jobConf = new JobConf(hconf, this.getClass)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        val tableName = "wanglei:" + args(0).split('/')(1)
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

        //将RDD转换成RRD[(ImmutableBytesWritable, Put)]类型
        val data = input.map{case(k,v) => convert(k,v)}
        //保存到HBase表
        data.saveAsHadoopDataset(jobConf)
        sc.stop()
    }
    //RDD转换函数
    def convert(k:(String,Int), v: Iterable[(String, Int)]) = {
        val rowkey = (k._1).reverse + k._2
        val put = new Put(Bytes.toBytes(rowkey))
        val iter = v.iterator
        while(iter.hasNext) {
            val pair = iter.next()
            put.addColumn(Bytes.toBytes("labels"), Bytes.toBytes(pair._1), Bytes.toBytes(pair._2))
        }
        (new ImmutableBytesWritable, put)
    }
}
```
### 二、使用saveAsNewAPIHadoopDataset
```scala
object SaveData {
    def main(args: Array[String]): Unit = {
        if(args.length < 1) {
            System.err.println("Usage: SaveData <input file>")
            System.exit(1)
        }
        val sconf = new SparkConf().setAppName("SaveRDDToHBase")
        val sc = new SparkContext(sconf)
        val input = sc.objectFile[((String,Int),Seq[(String,Int)])](args(0))

        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop04")

        val jobConf = new JobConf(hconf, this.getClass)
        //指定输出表
        val tableName = "wanglei:" + args(0).split('/')(1)
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
        //设置job的输出格式
        val job = new Job(jobConf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        val data = input.map{case(k,v) => convert(k,v)}
        //保存到HBase表
        data.saveAsNewAPIHadoopDataset(job.getConfiguration)
        sc.stop()
    }
}
```
### 三、遇到的问题
对于HBase集群是依赖Zookeeper集群的，所以需要在程序中设置hbase.zookeeper.quorum参数，否则出现在非Zookeeper server的节点上Spark任务无法运行的问题，查看错误日志输出如下
```scala
16/07/15 11:05:58 INFO zookeeper.ZooKeeper: Initiating client connection, connectString=localhost:2181 sessionTimeout=90000 watcher=hconnection-0x15e9fad80x0, quorum=localhost:2181, baseZNode=/hbase
16/07/15 11:05:58 INFO zookeeper.ZooKeeper: Initiating client connection, connectString=localhost:2181 sessionTimeout=90000 watcher=hconnection-0x4ac8153b0x0, quorum=localhost:2181, baseZNode=/hbase
16/07/15 11:05:58 INFO zookeeper.ZooKeeper: Initiating client connection, connectString=localhost:2181 sessionTimeout=90000 watcher=hconnection-0x1bfc34370x0, quorum=localhost:2181, baseZNode=/hbase
16/07/15 11:05:58 INFO zookeeper.ClientCnxn: Opening socket connection to server localhost/127.0.0.1:2181. Will not attempt to authenticate using SASL (unknown error)
16/07/15 11:05:58 INFO zookeeper.ClientCnxn: Opening socket connection to server localhost/127.0.0.1:2181. Will not attempt to authenticate using SASL (unknown error)
16/07/15 11:05:58 INFO zookeeper.ClientCnxn: Opening socket connection to server localhost/127.0.0.1:2181. Will not attempt to authenticate using SASL (unknown error)
16/07/15 11:05:58 WARN zookeeper.ClientCnxn: Session 0x0 for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:739)
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:350)
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1081)
```
可以看到因为没有设置zookeeper的quorum，所以该节点上的客户端默认去连接localhost，但localhost又不是zookeeper的quorum，所以连接失败。zookeeper客户端会反复重连，反复失败，该节点上的spark任务得不到执行。