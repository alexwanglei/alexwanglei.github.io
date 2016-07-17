title: Spark任务调度模式分析
date: 2016-04-28 09:19:57
categories: Spark
tags: [Spark]
---
本章分析Spark两种任务调度模式的实现过程。
<!-- more -->
## FIFO调度模式
TaskSchedulerImpl是负责任务调度的类，在SparkContext.createTaskScheduler方法中创建了scheduler:TaskSchedulerImpl和调度器后台backend:SparkDeploySchedulerBackend，然后执行scheduler.initialize(backend)：
```scala
def initialize(backend: SchedulerBackend) {
  this.backend = backend
  // temporarily set rootPool name to empty
  rootPool = new Pool("", schedulingMode, 0, 0)
  schedulableBuilder = {
    schedulingMode match {
      case SchedulingMode.FIFO =>
        new FIFOSchedulableBuilder(rootPool)
      case SchedulingMode.FAIR =>
        new FairSchedulableBuilder(rootPool, conf)
    }
  }
  schedulableBuilder.buildPools()
}
```
TaskScheduler.initialize方法创建了rootPool:Pool，类Pool是一个可调度的实体，代表Pools或TaskSetManagers集合。默认的调度模式schedulingMode=FIIO，根据调度模式，创建对应的schedulableBuilder:FIFOSchedulableBuilder(rootPool)。最后执行schedulableBuilder.buildPools()。
对于FIFOScheduleBuilder.buildPools():
```scala
override def buildPools() {
  // nothing
}
```
buildPools方法为空，不需要做什么。
当TaskSchedulerImpl调用submitTasks(taskSet: TaskSet)方法时：
```scala
override def submitTasks(taskSet: TaskSet) {
  ……
  this.synchronized {
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    ……
    schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
    ……
}
```
将taskSet创建为manager:TaskSetManager，然后执行schedulableBuilder.addTaskSetManager方法，对于FIFOScheduleBuilder.addTaskSetManager：
```scala
override def addTaskSetManager(manager: Schedulable, properties: Properties) {
  rootPool.addSchedulable(manager)
}
```
调用Pool.addSchedulable方法：
```scala
override def addSchedulable(schedulable: Schedulable) {
   require(schedulable != null)
   schedulableQueue.add(schedulable)
   schedulableNameToSchedulable.put(schedulable.name, schedulable)
   schedulable.parent = this
}
```
将manager:TaskSetManager添加到schedulableQueue:ConcurrentLinkedQueue[Schedulable]队列中。当需要调度TaskSetManager去执行时，调用Pool.getSortedTaskSetQueue方法：
```scala
override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
  var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
  val sortedSchedulableQueue =
    schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
  for (schedulable <- sortedSchedulableQueue) {
    sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
  }
  sortedTaskSetQueue
}
```
方法将schedulableQueue队列中的TaskSetManager按taskSetSchedulingAlgorithm.comparator算法比较排序，对于FIFO模式，taskSetSchedulingAlgorithm为FIFOSchedulingAlgorithm，comparator方法：
```scala
override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
  val priority1 = s1.priority
  val priority2 = s2.priority
  var res = math.signum(priority1 - priority2)
  if (res == 0) {
    val stageId1 = s1.stageId
    val stageId2 = s2.stageId
    res = math.signum(stageId1 - stageId2)
  }
  if (res < 0) {
    true
  } else {
    false
  }
}
```
comparator方法首先比较两个TaskSetManager的优先级，TaskSetManager.priority=TaskSet.priority=Stage.firstJobId=JobId，由于一个job会产生多个stage，它们的firstJobId都是JobId，因此一个job的TaskSet.priority都相同。优先级相同，所以继续比较stageId，stageId是按照stage的父子依赖关系生成的，父stage的stageId更小，需要优先计算。
## FAIR调度模式
对于FAIR调度策略，schedulableBuilder为FairSchedulableBuilder，buildPools方法：
```scala
override def buildPools() {
  var is: Option[InputStream] = None
  try {
    is = Option {
      schedulerAllocFile.map { f =>
        new FileInputStream(f)
      }.getOrElse {
        Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
      }
    }

    is.foreach { i => buildFairSchedulerPool(i) }
  } finally {
    is.foreach(_.close())
  }

  // finally create "default" pool
  buildDefaultPool()
}
```
buildPools方法根据schedulerAllocFile创建文件输入流，schedulerAllocFile是由属性"spark.scheduler.allocation.file"指定的文件名，如果没有设置该属性，则默认使用DEFAULT_SCHEDULER_FILE="fairscheduler.xml"文件，然后对文件输入流调用buildFairSchedulerPool(i)：
```scala
private def buildFairSchedulerPool(is: InputStream) {
  val xml = XML.load(is)
  for (poolNode <- (xml \\ POOLS_PROPERTY)) {
    val poolName = (poolNode \ POOL_NAME_PROPERTY).text
    var schedulingMode = DEFAULT_SCHEDULING_MODE
    var minShare = DEFAULT_MINIMUM_SHARE
    var weight = DEFAULT_WEIGHT

    val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text
    if (xmlSchedulingMode != "") {
      try {
        schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
      } catch {
        case e: NoSuchElementException =>
          logWarning("Error xml schedulingMode, using default schedulingMode")
      }
    }

    val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text
    if (xmlMinShare != "") {
      minShare = xmlMinShare.toInt
    }

    val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text
    if (xmlWeight != "") {
      weight = xmlWeight.toInt
    }

    val pool = new Pool(poolName, schedulingMode, minShare, weight)
    rootPool.addSchedulable(pool)
    logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
      poolName, schedulingMode, minShare, weight))
  }
}
```
buildFairSchedulerPool方法解析文件输入流对应的xml文档，根据xml文档定义创建Pool，然后将创建的pool都添加到rootPool的schedulableQueue队列中。
buildPools方法根据xml文件创建完pool后，最后还要创建默认的pool，buildDefaultPool()：
```scala
private def buildDefaultPool() {
  if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
    val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
      DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
    rootPool.addSchedulable(pool)
    logInfo("Created default pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
      DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
  }
}
```
对于FAIRScheduleBuilder.addTaskSetManager：
```scala
override def addTaskSetManager(manager: Schedulable, properties: Properties) {
  var poolName = DEFAULT_POOL_NAME
  var parentPool = rootPool.getSchedulableByName(poolName)
  if (properties != null) {
    poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
    parentPool = rootPool.getSchedulableByName(poolName)
    if (parentPool == null) {
      // we will create a new pool that user has configured in app
      // instead of being defined in xml file
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(parentPool)
      logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }
  parentPool.addSchedulable(manager)
  logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
}
```
addTaskSetManager会根据properties将TaskSetManager添加到指定poolName的子pool中，如果rootPool下面没有该名称得子pool则创建该子pool。如果未指定properties则默认添加到defaultPool中。
当需要调度TaskSetManager去执行时，调用Pool.getSortedTaskSetQueue方法：
```scala
override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
  var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
  val sortedSchedulableQueue =
    schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
  for (schedulable <- sortedSchedulableQueue) {
    sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
  }
  sortedTaskSetQueue
}
```
在FAIR调度模式下，rootPool的调度队列中是自定义的带名字的子pool和defaultPool，在这些子pool的调度队列中才是TaskSetManager，因此rootPool.getSortedTaskSetQueue方法先根据FAIRSchedulingAlgorithm算法对子pool进行排序，然后按序对每个子pool再调用getSortedTaskSetQueue，子pool按照自己的调度模式再对调度队列中的TaskSetManager排序。最后将全部的TaskSetManager排序。
taskSetSchedulingAlgorithm为FAIRSchedulingAlgorithm，comparator方法：
```scala
override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
  val minShare1 = s1.minShare
  val minShare2 = s2.minShare
  val runningTasks1 = s1.runningTasks
  val runningTasks2 = s2.runningTasks
  val s1Needy = runningTasks1 < minShare1
  val s2Needy = runningTasks2 < minShare2
  val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0).toDouble
  val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0).toDouble
  val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
  val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble
  var compare: Int = 0

  if (s1Needy && !s2Needy) {
    return true
  } else if (!s1Needy && s2Needy) {
    return false
  } else if (s1Needy && s2Needy) {
    compare = minShareRatio1.compareTo(minShareRatio2)
  } else {
    compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
  }

  if (compare < 0) {
    true
  } else if (compare > 0) {
    false
  } else {
    s1.name < s2.name
  }
}
 ```
 该comparator方法对两个pool进行比较，根据pool的minShare，runningTaskings和weight三个属性确定pool的排序。minShare表示该pool持有的最小共享资源（CPU核数）；runningTaskings表示该pool中正在运行的task个数；weight表示该pool占有共享资源的比例。