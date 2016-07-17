title: Standalone Client模式提交应用运行流程分析
date: 2016-04-25 15:28:16
categories: Spark
tags: [Spark]
---
在Spark Standalone集群模式下， 通过Client提交Spark应用程序到集群上执行。本章对提交应用的过程进行分析。
<!-- more -->
* Standalone：Spark自带的集群资源管理模式，主从架构，一个主机是Master，上面运行了Master守护进程；其他主机是Worker，上面运行了Worker守护进程。
* Client：应用部署的方式，使用submit命令提交应用的时候，使用参数 --deploy-mode=client来指定将驱动程序运行在本机，即提交应用使用的这台机器，也称为客户机。

## 使用spark-submit脚本提交应用程序
例如运行spark example中的计算π的程序：
```ps
./bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://hadoop01:7077 \
--executor-memory 20G \
--total-executor-cores 100 \
/usr/lib/spark/examples/lib/spark-examples-1.3.0-cdh5.4.8-hadoop2.6.0-cdh5.4.8.jar \
1000
```
参数 --class指定运行应用的主类；--master指定spark master的地址；通常还有--deploy-mode指定应用部署模式，缺省的话为client。还可以配置一些计算资源参数。最后是应用的jar文件，后面跟应用的参数。
spark-submit脚本执行：
```ps
exec "$SPARK_HOME"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```
注：$@表示脚本的参数列表
spark-class脚本执行命令：
```ps
exec /usr/java/latest/bin/java 
-cp :/usr/lib/spark/conf:各种jar 
-XX:MaxPermSize=128m -Xms512m -Xmx512m 
org.apache.spark.deploy.SparkSubmit 
--class org.apache.spark.examples.SparkPi 
--master spark://hadoop01:7077 
--executor-memory 20G 
--total-executor-cores 100 
/usr/lib/spark/examples/lib/spark-examples-1.3.0-cdh5.4.8-hadoop2.6.0-cdh5.4.8.jar 1000
```
## 应用提交类——org.apache.spark.deploy.SparkSubmit
### SparkSubmit main方法
SparkSubmit是Scala Object，main方法中，首先将参数args构造成SparkSubmitArguments对象，然后调用submit方法。
```scala
def main(args: Array[String]): Unit = {
  val appArgs = new SparkSubmitArguments(args)
  if (appArgs.verbose) {
    // scalastyle:off println
    printStream.println(appArgs)
    // scalastyle:on println
  }
  appArgs.action match {
    case SparkSubmitAction.SUBMIT => submit(appArgs)
    case SparkSubmitAction.KILL => kill(appArgs)
    case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
  }
}
```
在本例中，参数被解析为SparkSubmitArguments对象的成员变量：
- appArgs.mainClass=org.apache.spark.examples.SparkPi
- appArgs.master=spark://hadoop01:7077
- appArgs.executorMemor=20G
- appArgs.totalExecutorCores=100
- appArgs.primaryResource=file:///usr/lib/spark/examples/lib/spark-examples-1.3.0-cdh5.4.8-hadoop2.6.0-cdh5.4.8.jar
- appArgs.childArgs=1000
- appArgs.action=SUBMIT

### submit方法
首先准备启动环境，设置合适的classpath，系统属性和应用参数。使用prepareSubmitEnvironment方法实现。
```scala
val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)
```
返回值：
- childArgs=appArgs.childArgs
- childClassPath=appArgs.primaryResource
- sysProps是系统属性配置
- childMainClass=appArgs.mainClass

然后调用runMain方法：
```scala
runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
```
### runMain方法
利用反射调用childMainClass的main方法，即用户编写的应用程序的main方法。
```scala
……
mainClass = Class.forName(childMainClass, true, loader)
……
val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
……
mainMethod.invoke(null, childArgs.toArray)
```
本例中mainClass为SparkPi，main方法中是计算π的具体代码：
```scala
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
```