title: Slave启动过程
date: 2016-04-21 13:48:40
categories: Spark
tags: [Spark]
---

Spark Standalone部署模式是主从结构，有一个主节点，其余为从节点，从节点机器上需要启动Slave服务。
<!-- more -->
## 启动slaves的脚本: start-slaves.sh
脚本调用sbin/slaves.sh，在conf/slaves文件中指定的每台机器上启动一个slave实例。
```ps
"$sbin/slaves.sh" cd "$SPARK_HOME" \; 
"$sbin/start-slave.sh" "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
```
slaves.sh从conf/slaves文件中读取slave主机的ip地址，然后ssh登录到slave主机，切换到spark_home目录，执行**sbin/start-slave.sh**，该脚本同样调用spark-deamon.sh启动work守护进程。
```ps
"$sbin"/spark-daemon.sh start org.apache.spark.deploy.worker.Worker $WORKER_NUM \
 --webui-port "$WEBUI_PORT" $PORT_FLAG $PORT_NUM $MASTER "$@"
```
## 启动守护进程的脚本: spark-daemon.sh
脚本调用spark-class最终执行：
```ps
/bin/spark-class org.apache.spark.deploy.worker.Worker spark://master_ip:7077
```
## 启动具体scala类的脚本: spark-class.sh
脚本最终执行：
```ps
exec /usr/java/latest/bin/java -cp :各种lib: 
-XX:MaxPermSize=128m -Dspark.akka.logLifecycleEvents=true 
-Xms512m -Xmx512m org.apache.spark.deploy.worker.Worker spark://hadoop01:7077
```
脚本设置java运行程序：`RUNNER="${JAVA_HOME}/bin/java"` ，然后准备scala环境和库，以及各种依赖的jar包。最后运行java命令执行`org.apache.spark.deploy.worker.Worker`类的main方法。
## Worker守护进程的的scala类: org.apache.spark.deploy.worker.Worker
Worker类代表了spark worker对象，继承了Actor类，是Akka中的通信角色。
Worker伴生对象的main方法，将参数构造成WorkerArguments对象，然后调用startSystemAndActor方法创建Worker Actor。

```scala
def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    actorSystem.awaitTermination()
}
```