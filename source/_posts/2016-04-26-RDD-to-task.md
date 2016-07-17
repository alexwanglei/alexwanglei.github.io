title: RDD task的生成过程
date: 2016-04-26 15:56:18
categories: Spark
tags: [Spark]
---
本章分析对RDD操作生成task的具体执行过程。
<!-- more -->
## job => stage
对RDD操作包括transform操作和action操作，对transform操作，由于是惰性求值，不会触发计算，直到最后action操作时，会触发运行Job，例如count操作：
```scala
def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum
```
SparkContext类中重载了runJob方法，最终都会调用下面的runJob函数，它是所有action操作的主入口：
```scala
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit): Unit = {
  if (stopped.get()) {
    throw new IllegalStateException("SparkContext has been shutdown")
  }
  val callSite = getCallSite
  val cleanedFunc = clean(func)
  logInfo("Starting job: " + callSite.shortForm)
  if (conf.getBoolean("spark.logLineage", false)) {
    logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
  }
  dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
  progressBar.foreach(_.finishAll())
  rdd.doCheckpoint()
}
```
DagScheduler类是实现面向stage调度的高层调度层。它为每个job计算一个stages DAG，记录具体化的RDDs和stage输出，找出最优调度运行job。然后将stages作为TaskSets提交给底层TaskScheduler的具体实现，由TaskSchedulerImpl调度TaskSets运行到集群上。SparkContext.runJob调用dagScheduler.runJob方法：
```scala
def runJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): Unit = {
  val start = System.nanoTime
  val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
  waiter.awaitResult() match {
    case JobSucceeded =>
      logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
    case JobFailed(exception: Exception) =>
      logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
      val callerStackTrace = Thread.currentThread().getStackTrace.tail
      exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
      throw exception
  }
}
```
DagScheduler.runJob方法中调用DagScheduler.submitJob方法：
```scala
def submitJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): JobWaiter[U] = {
  // Check to make sure we are not launching a task on a partition that does not exist.
  val maxPartitions = rdd.partitions.length
  partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
    throw new IllegalArgumentException(
      "Attempting to access a non-existent partition: " + p + ". " +
        "Total number of partitions: " + maxPartitions)
  }

  val jobId = nextJobId.getAndIncrement()
  if (partitions.size == 0) {
    return new JobWaiter[U](this, jobId, 0, resultHandler)
  }

  assert(partitions.size > 0)
  val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
  val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
  eventProcessLoop.post(JobSubmitted(
    jobId, rdd, func2, partitions.toArray, callSite, waiter,
    SerializationUtils.clone(properties)))
waiter
}
```
submitJob方法通过eventProcessLoop.post(JobSubmitted)往事件队列里增加一个JobSubmitted事件。eventProcessLoop是DAGSchedulerEventProcessLoop类实例，该类又继承EventLoop抽象类，该类中有一个守护线程，从eventQueue中取event然后调用onRecevie(event)。DAGSchedulerEventProcessLoop类中onReceive方法：
```scala
override def onReceive(event: DAGSchedulerEvent): Unit = {
  val timerContext = timer.time()
  try {
    doOnReceive(event)
  } finally {
    timerContext.stop()
  }
}
```
调用doOnReceive方法：
```scala
private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
  case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
    dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
  ……
}
```
对于JobSubmitted事件，执行dagScheduler.handleJobSubmitted方法：
```scala
private[scheduler] def handleJobSubmitted(jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties) {
  var finalStage: ResultStage = null
  try {
    finalStage = newResultStage(finalRDD, partitions.length, jobId, callSite)
  } 
  ……
  if (finalStage != null) {
    val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
    clearCacheLocs()
    ……
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.resultOfJob = Some(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }
  submitWaitingStages()
}
```
## stage => taskSet
在DagScheduler.handleJobSubmitted方法中调用newResultStage()方法获取最终的ResultStage，期间会通过getParentStagesAndId来生成父stage。最后调用submitStage(finalStage):
```scala
private def submitStage(stage: Stage) {
  val jobId = activeJobForStage(stage)
  if (jobId.isDefined) {
    logDebug("submitStage(" + stage + ")")
    if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
      val missing = getMissingParentStages(stage).sortBy(_.id)
      logDebug("missing: " + missing)
      if (missing.isEmpty) {
        logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
        submitMissingTasks(stage, jobId.get)
      } else {
        for (parent <- missing) {
          submitStage(parent)
        }
        waitingStages += stage
      }
    }
  } else {
    abortStage(stage, "No active job for stage " + stage.id, None)
  }
}
```
submitStage方法会递归的提交stage的父stage，直到没有missParentStages(缺少的父stages)，然后调用submitMissingTasks():
```scala
private def submitMissingTasks(stage: Stage, jobId: Int) {
  ……
  var taskBinary: Broadcast[Array[Byte]] = null
  try {
    // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
    // For ResultTask, serialize and broadcast (rdd, func).
    val taskBinaryBytes: Array[Byte] = stage match {
      case stage: ShuffleMapStage =>
        closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
      case stage: ResultStage =>
        closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
    }

    taskBinary = sc.broadcast(taskBinaryBytes)
  }
  ……
  val tasks: Seq[Task[_]] = try {
    stage match {
      case stage: ShuffleMapStage =>
        partitionsToCompute.map { id =>
          val locs = taskIdToLocations(id)
          val part = stage.rdd.partitions(id)
          new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
            taskBinary, part, locs, stage.internalAccumulators)
        }

      case stage: ResultStage =>
        val job = stage.resultOfJob.get
        partitionsToCompute.map { id =>
          val p: Int = job.partitions(id)
          val part = stage.rdd.partitions(p)
          val locs = taskIdToLocations(id)
          new ResultTask(stage.id, stage.latestInfo.attemptId,
            taskBinary, part, locs, id, stage.internalAccumulators)
        }
    }
  }
  ……
  if (tasks.size > 0) {
    logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
    stage.pendingPartitions ++= tasks.map(_.partitionId)
    logDebug("New pending partitions: " + stage.pendingPartitions)
    taskScheduler.submitTasks(new TaskSet(
      tasks.toArray, stage.id, stage.latestInfo.attemptId, stage.firstJobId, properties))
    stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
  }
  ……
}
```
subMissingTasks方法将stage转换成TaskSet，然后交给taskScheduler提交。Stage分为ShuffleMapStage和ResultStage，对于ShuffleMapStage，将Stage的rdd和shuffleDep序列化为taskBinaryBytes；对于ResultStage，将Stage的rdd和func序列化为taskBinaryBytes。将taskBinaryBytes封装成广播变量taskBinary。对于ShuffleMapStage，为每个需要计算的分区创建一个ShuffleMapTask，最后得到一个ShuffleMapTask序列；对于ResultStage，为每个需要计算的分区创建一个ResultTask，最后得到一个ResultTask序列。根据Task序列和stage的信息创建TaskSet对象，最后执行taskScheduler.submitTasks(TaskSet)：
```scala
override def submitTasks(taskSet: TaskSet) {
  val tasks = taskSet.tasks
  logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
  this.synchronized {
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    val stage = taskSet.stageId
    val stageTaskSets =
      taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
    stageTaskSets(taskSet.stageAttemptId) = manager
    val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
      ts.taskSet != taskSet && !ts.isZombie
    }
    if (conflictingTaskSet) {
      throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
        s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
    }
    schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

    if (!isLocal && !hasReceivedTask) {
      starvationTimer.scheduleAtFixedRate(new TimerTask() {
        override def run() {
          if (!hasLaunchedTask) {
            logWarning("Initial job has not accepted any resources; " +
              "check your cluster UI to ensure that workers are registered " +
              "and have sufficient resources")
          } else {
            this.cancel()
          }
        }
      }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
    }
    hasReceivedTask = true
  }
  backend.reviveOffers()
}
```
TaskSchedulerImpl.submitTasks方法调用createTaskSetManager方法创建TaskSetManager对象，管理TaskSet中的tasks。然后执行schedulableBuilder.addTaskSetManager方法将TaskSetManager对象添加到调度池(pool)中。**关于任务调度池后续单独分析**。最后执行backend.reviveOffers()
## TaskSet => Task
backend是SparkDeploySchedulerBackend对象，父类CoarseGrainedSchedulerBackend.reviveOffers()方法：
```scala
override def reviveOffers() {
  driverEndpoint.send(ReviveOffers)
}
```
reviveOffers方法向DriverEndpoint发送ReviveOffers消息。DriverEndpoint收到ReviveOffers消息处理：
```scala
override def receive: PartialFunction[Any, Unit] = {
  ……
  case ReviveOffers =>
    makeOffers()
  ……
```
调用makeOffers()方法：
```scala
private def makeOffers() {
  // Filter out executors under killing
  val activeExecutors = executorDataMap.filterKeys(!executorsPendingToRemove.contains(_))
  val workOffers = activeExecutors.map { case (id, executorData) =>
    new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
  }.toSeq
  launchTasks(scheduler.resourceOffers(workOffers))
}
```
makeOffers方法获取可用的Executor资源，以WorkerOffer对象表示。然后执行scheduler.resourceOffers(workOffers)获取任务描述：
```scala
def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
  ……
  // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
  val shuffledOffers = Random.shuffle(offers)
  // Build a list of tasks to assign to each worker.
  val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
  val availableCpus = shuffledOffers.map(o => o.cores).toArray
  val sortedTaskSets = rootPool.getSortedTaskSetQueue
  for (taskSet <- sortedTaskSets) {
    logDebug("parentName: %s, name: %s, runningTasks: %s".format(
      taskSet.parent.name, taskSet.name, taskSet.runningTasks))
    if (newExecAvail) {
      taskSet.executorAdded()
    }
  }

  // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
  // of locality levels so that it gets a chance to launch local tasks on all of them.
  // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
  var launchedTask = false
  for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
    do {
      launchedTask = resourceOfferSingleTaskSet(
          taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
    } while (launchedTask)
  }

  if (tasks.size > 0) {
    hasLaunchedTask = true
  }
  return tasks
}
```
TaskSchedulerImpl.makeOffers方法随机打乱Executor资源offers，然后为每个offer创建一个TaskDescription数组，数组长度为offer的核数。通过rootPool.getSortedTaskSetQueue返回按调度策略排序的TaskSets。对每个TaskSet，根据taskSet.myLocalityLevels，执行resourceOfferSingleTaskSet方法：
```scala
private def resourceOfferSingleTaskSet(
    taskSet: TaskSetManager,
    maxLocality: TaskLocality,
    shuffledOffers: Seq[WorkerOffer],
    availableCpus: Array[Int],
    tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
  var launchedTask = false
  for (i <- 0 until shuffledOffers.size) {
    val execId = shuffledOffers(i).executorId
    val host = shuffledOffers(i).host
    if (availableCpus(i) >= CPUS_PER_TASK) {
      try {
        for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
          tasks(i) += task
          val tid = task.taskId
          taskIdToTaskSetManager(tid) = taskSet
          taskIdToExecutorId(tid) = execId
          executorsByHost(host) += execId
          availableCpus(i) -= CPUS_PER_TASK
          assert(availableCpus(i) >= 0)
          launchedTask = true
        }
      } catch {
        case e: TaskNotSerializableException =>
          logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
          // Do not offer resources for this task, but don't throw an error to allow other
          // task sets to be submitted.
          return launchedTask
      }
    }
  }
  return launchedTask
}
```
TaskSchdulerImpl.resourceOfferSingleTaskSet通过调用taskSet.resourceOffer，为TaskDescription数组的数组tasks填充元素。
回到CoarseGrainedSchedulerBackend.makeOffers()方法，launchTasks(scheduler.resourceOffers(workOffers))即launchTasks(tasks)：
```scala
private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
  for (task <- tasks.flatten) {
    val serializedTask = ser.serialize(task)
    if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
      scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
        try {
          var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
            "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
            "spark.akka.frameSize or using broadcast variables for large values."
          msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
            AkkaUtils.reservedSizeBytes)
          taskSetMgr.abort(msg)
        } catch {
          case e: Exception => logError("Exception in error callback", e)
        }
      }
    }
    else {
      val executorData = executorDataMap(task.executorId)
      executorData.freeCores -= scheduler.CPUS_PER_TASK
      executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
    }
  }
}
```
launchTasks方法将task序列化，然后向执行该任务的executorEndpoint发送LaunchTask消息。
## executor启动task
CoarseGrainedExecutorBackend收到LaunchTask消息：
```scala
case LaunchTask(data) =>
  if (executor == null) {
    logError("Received LaunchTask command but executor was null")
    System.exit(1)
  } else {
    val taskDesc = ser.deserialize[TaskDescription](data.value)
    logInfo("Got assigned task " + taskDesc.taskId)
    executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
      taskDesc.name, taskDesc.serializedTask)
  }
```
执行Executor.launchTask方法：
```scala
def launchTask(
    context: ExecutorBackend,
    taskId: Long,
    attemptNumber: Int,
    taskName: String,
    serializedTask: ByteBuffer): Unit = {
  val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
    serializedTask)
  runningTasks.put(taskId, tr)
  threadPool.execute(tr)
}
```
Executor.launchTask方法创建TaskRunner对象tr，然后调用threadPool.execute(tr)启动线程运行TaskRunner。
TaskRunner继承Runnable，run()方法：
```scala
override def run(): Unit = {
  ……
   try {
    val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
    updateDependencies(taskFiles, taskJars)
    task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
    task.setTaskMemoryManager(taskMemoryManager)
    ……
    val (value, accumUpdates) = try {
      val res = task.run(
        taskAttemptId = taskId,
        attemptNumber = attemptNumber,
        metricsSystem = env.metricsSystem)
      threwException = false
      res
    }
    ……
}
```
TaskRunner.run方法将序列化的任务serializedTask反序列化，得到Task对象，然后调用Task.run方法：
```scala
final def run(
  taskAttemptId: Long,
  attemptNumber: Int,
  metricsSystem: MetricsSystem)
: (T, AccumulatorUpdates) = {
  context = new TaskContextImpl(
    stageId,
    partitionId,
    taskAttemptId,
    attemptNumber,
    taskMemoryManager,
    metricsSystem,
    internalAccumulators,
    runningLocally = false)
  TaskContext.setTaskContext(context)
  context.taskMetrics.setHostname(Utils.localHostName())
  context.taskMetrics.setAccumulatorsUpdater(context.collectInternalAccumulators)
  taskThread = Thread.currentThread()
  if (_killed) {
    kill(interruptThread = false)
  }
  try {
    (runTask(context), context.collectAccumulators())
  } finally {
    context.markTaskCompleted()
    try {
      Utils.tryLogNonFatalError {
        // Release memory used by this thread for shuffles
        SparkEnv.get.shuffleMemoryManager.releaseMemoryForThisTask()
      }
      Utils.tryLogNonFatalError {
        // Release memory used by this thread for unrolling blocks
        SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask()
      }
    } finally {
      TaskContext.unset()
    }
  }
}
```
Task.run方法创建了TaskContextImpl对象，然后调用runTask方法。Task类是抽象类，runTask没有实现，Task有两个具体实现ShuffleMapTask和ResultTask，它们分别实现了自己的runTask方法。