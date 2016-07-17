title: Master启动过程
date: 2016-04-21 10:26:44
categories: Spark
tags: [Spark]
---

Spark Standalone部署模式是主从结构，有一个主节点，其余为从节点，主节点机器上需要启动Master服务。
<!-- more -->
## 启动Master的shell脚本: start-master.sh
运行start-master.sh启动spark master守护进程。脚本首先载入***sbin/spark-config.sh***和***bin/load-spark-env.sh***:

* spark-config.sh配置spark_home, spark_conf环境变量，指向对应的目录。
* load-spark-env.sh载入conf/spark-env.sh脚本，spark-env.sh中可以配置各种环境变量和依赖的lib路径。

然后设置spark master的端口为7077和IP为hostname，以及master webui的端口为8080。最后调用spark-deamon.sh脚本。

```ps
. "$sbin/spark-config.sh" 

. "$SPARK_PREFIX/bin/load-spark-env.sh"

if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi
if [ "$SPARK_MASTER_IP" = "" ]; then
	  SPARK_MASTER_IP=`hostname`
fi
if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi

"$sbin"/spark-daemon.sh start org.apache.spark.deploy.master.Master 1 
--ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT 
--webui-port $SPARK_MASTER_WEBUI_PORT $ORIGINAL_ARGS
```
## 启动守护进程的脚本: spark-daemon.sh
该脚本的用法：
```ps
usage="Usage: spark-daemon.sh [--config <conf-dir>] (start|stop|status) <spark-command> <spark-instance-number> <args...>"
```
脚本先做一些准备工作，设置log和pid，然后根据参数调用spark-class脚本，最终执行的命令为：
```ps 
bin/spark-class org.apache.spark.deploy.master.Master --ip hostname --port 7077 --webui-port 8080
```
## 启动具体scala类的脚本: spark-class.sh
该脚本的用法：
```ps 	
"Usage: spark-class <class> [<args>]"
```
脚本设置java运行程序：`RUNNER="${JAVA_HOME}/bin/java"` ，然后准备scala环境和库，以及各种依赖的jar包。最后运行java命令执行`org.apache.spark.deploy.master.Master`类的main方法。
 ```ps    
exec /usr/java/latest/bin/java -cp ：各种jar包： -XX:MaxPermSize=128m -Dspark.akka.logLifecycleEvents=true -Xms512m -Xmx512m org.apache.spark.deploy.master.Master --ip hadoop01 --port 7077 --webui-port 8080
```
## Master守护进程的scala类: org.apache.spark.deploy.master.Master
Master类伴生对象的main方法是程序执行入口：首先将参数包装成MasterArguments对象，然后调用**startSystemAndActor**方法。
```scala
def main(argStrings: Array[String]) {
  SignalLogger.register(log)
  val conf = new SparkConf
  val args = new MasterArguments(argStrings, conf)
  val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port,args.webUiPort, conf)
  actorSystem.awaitTermination()
}
```
startSystemAndActor方法启动Master，并返回一个四元组：

1. Master actor system
2. 绑定的端口
3. web UI绑定的端口
4. REST服务绑定的端口，如果有

代码
```scala
def startSystemAndActor(
  host: String,
  port: Int,
  webUiPort: Int,
  conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
  val securityMgr = new SecurityManager(conf)
  val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,securityManager = securityMgr)
  val actor = actorSystem.actorOf(Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)
  val timeout = RpcUtils.askTimeout(conf)
  val portsRequest = actor.ask(BoundPortsRequest)(timeout)
  val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
  (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
}
```
通过actorSystem.actorOf方法创建Master的actor对象，Master类继承了Actor，因此Master是Akka中的通信角色。Actor创建时，会先运行preStart方法，在Master的preStart方法中会启动一些其他的组件。
Master Actor启动后，就会等待接收来自其他Actor的消息，对不同的消息作出相应的响应动作。

