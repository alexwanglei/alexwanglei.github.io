title: 应用执行过程分析
date: 2016-04-25 16:23:59
categories: Spark
tags: [Spark]
---
本章分析Spark应用程序在创建SparkContext过程中准备环境和分配计算资源的过程。Spark应用程序首先创建SparkContext对象，然后载入数据生成RDD，对RDD进行transform操作，最后进行action操作。
<!-- more -->
## 创建SparkContext对象
SparkContext是使用Spark的主入口，它是连接到Spark集群的接口，使用它可以创建RDD，accumulators和broadcast变量。
在SparkContext对象的构造过程中，创建任务调度器和调度器后台，并启动：
```scala
val (sched, ts) = SparkContext.createTaskScheduler(this, master)
_schedulerBackend = sched
_taskScheduler = ts
……
_taskScheduler.start()
```
createTaskScheduler方法根据master URL创建对应集群运行模式的调度器后台。对于Standalone模式，调度器后台为SparkDeploySchedulerBackend对象。
任务调度器为TaskSchedulerImpl对象，通过start方法启动任务调度器，在方法中会执行调度器后台的start方法，启动调度器后台。然后以100ms为周期运行**checkSpeculatableTasks()**方法。该方法后续分析！！！
```scala
override def start() {
  backend.start()

  if (!isLocal && conf.getBoolean("spark.speculation", false)) {
    logInfo("Starting speculative execution thread")
    speculationScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
        checkSpeculatableTasks()
      }
    }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
  }
}
```
调度器后台是SparkDeploySchedulerBackend，继承了CoarseGrainedSchedulerBackend。SparkDeploySchedulerBackend.start()方法：
```scala
override def start() {
  super.start()
  // The endpoint for executors to talk to us
  val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
    RpcAddress(sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port").toInt),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
  val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
  ……
  val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
    args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
  val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
  val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
  val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory,
    command, appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor)
  client = new AppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
  client.start()
  waitForRegistration()
}
```
首先调用父类的start方法，CoarseGrainedSchedulerBackend.start()方法创建了名为“CoarseGrainedScheduler”的DriverEndpoint。
```scala
override def start() {
  val properties = new ArrayBuffer[(String, String)]
  for ((key, value) <- scheduler.sc.conf.getAll) {
    if (key.startsWith("spark.")) {
      properties += ((key, value))
    }
  }
  // TODO (prashant) send conf instead of properties
  driverEndpoint = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME, new DriverEndpoint(rpcEnv, properties))
}
```
继续SparkDeploySchedulerBackend.start()方法，调用完父类的start方法后，准备好driverUrl，args，command等变量，然后构造ApplicationDescription对象。最后创建了AppClient对象并启动。
AppClient.start()方法：创建名为“AppClient”的ClientEndpoint
```scala
def start() {
  // Just launch an rpcEndpoint; it will call back into the listener.
  endpoint = rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv))
}
```
RpcEndpoint创建完后会执行onStart方法，ClientEndpoint.onStart()方法：
```scala
override def onStart(): Unit = {
  try {
    registerWithMaster(1)
  } catch {
    case e: Exception =>
      logWarning("Failed to connect to master", e)
      markDisconnected()
      stop()
  }
}
```
onStart方法调用registerWithMaster()方法：
```scala
private def registerWithMaster(nthRetry: Int) {
  registerMasterFutures = tryRegisterAllMasters()
  registrationRetryTimer = registrationRetryThread.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      Utils.tryOrExit {
        if (registered) {
          registerMasterFutures.foreach(_.cancel(true))
          registerMasterThreadPool.shutdownNow()
        } else if (nthRetry >= REGISTRATION_RETRIES) {
          markDead("All masters are unresponsive! Giving up.")
        } else {
          registerMasterFutures.foreach(_.cancel(true))
          registerWithMaster(nthRetry + 1)
        }
      }
    }
  }, REGISTRATION_TIMEOUT_SECONDS, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
}
```
registerWithMaster()方法调用tryRegisterAllMasters()方法：
```scala
private def tryRegisterAllMasters(): Array[JFuture[_]] = {
  for (masterAddress <- masterRpcAddresses) yield {
    registerMasterThreadPool.submit(new Runnable {
      override def run(): Unit = try {
        if (registered) {
          return
        }
        logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
        val masterRef =
          rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
        masterRef.send(RegisterApplication(appDescription, self))
      } catch {
        case ie: InterruptedException => // Cancelled
        case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
      }
    })
  }
}
```
获取masterRef，向masterEndpoint发送RegisterApplication(appDescription, self)消息。
小结：SparkContext创建并启动调度器和调度器后台，调度器负责调度任务，调度器后台负责协调资源。调度器后台创建了两个RpcEndpoint，分别是DriverEndpoint和ClientEndpoint。ClientEndpoint与MasterEndpoint通信，将应用程序的信息发送给Master。
## Master在Workers上分配Executors
Master收到RegisterApplication消息进行处理：
```scala
case RegisterApplication(description, driver) => {
  // TODO Prevent repeated registrations from some driver
  if (state == RecoveryState.STANDBY) {
    // ignore, don't send response
  } else {
    logInfo("Registering app " + description.name)
    val app = createApplication(description, driver)
    registerApplication(app)
    logInfo("Registered app " + description.name + " with ID " + app.id)
    persistenceEngine.addApplication(app)
    driver.send(RegisteredApplication(app.id, self))
    schedule()
  }
}
```
首先创建ApplicationInfo对象app，然后注册app，给ClientEndpoint发送RegisteredApplication消息，最后调用schedule()方法。
```scala
private def schedule(): Unit = {
  if (state != RecoveryState.ALIVE) { return }
  // Drivers take strict precedence over executors
  val shuffledWorkers = Random.shuffle(workers) // Randomization helps balance drivers
  for (worker <- shuffledWorkers if worker.state == WorkerState.ALIVE) {
    for (driver <- waitingDrivers) {
      if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
        launchDriver(worker, driver)
        waitingDrivers -= driver
      }
    }
  }
  startExecutorsOnWorkers()
}
```
Master的Schedule()方法给等待中的应用程序调度可用的资源。当新应用到来或可用资源变化时就会被调用。本例中由于是deploy=Client模式，driver在客户端机器上，对应ClientEndpoint。所以waitingDrivers为空。当deploy=Cluster模式时，waitingDrivers会被随机分配到worker上。因此循环会跳过，只执行startExecutorsOnWorkers()方法：
```scala
private def startExecutorsOnWorkers(): Unit = {
  // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
  // in the queue, then the second app, etc.
  for (app <- waitingApps if app.coresLeft > 0) {
    val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
    // Filter out workers that don't have enough resources to launch an executor
    val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
      .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
        worker.coresFree >= coresPerExecutor.getOrElse(1))
      .sortBy(_.coresFree).reverse
    val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

    // Now that we've decided how many cores to allocate on each worker, let's allocate them
    for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
      allocateWorkerResourceToExecutors(
        app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
    }
  }
}
```
startExecutorsOnWokers()方法调度Executors在workers上启动，采用默认的spread out模式，将应用的executors分配到尽可能多的workers上，先进行每个worker上分配的核数的计算，最后调用allocateWorkerResourceToExecutors()方法：
```scala
private def allocateWorkerResourceToExecutors(
    app: ApplicationInfo,
    assignedCores: Int,
    coresPerExecutor: Option[Int],
    worker: WorkerInfo): Unit = {
  // If the number of cores per executor is specified, we divide the cores assigned
  // to this worker evenly among the executors with no remainder.
  // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
  val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
  val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
  for (i <- 1 to numExecutors) {
    val exec = app.addExecutor(worker, coresToAssign)
    launchExecutor(worker, exec)
    app.state = ApplicationState.RUNNING
  }
}
```
allocateWorkerResourceToExecutros方法首先计算该worker上可创建的executor数目(使用worker上可用核数除以每个Executor指定的核数)，然后创建ExecutorDesc，调用launchExecutor方法在worker上启动Executor：
```scala
private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
  logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
  worker.addExecutor(exec)
  worker.endpoint.send(LaunchExecutor(masterUrl,
    exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
  exec.application.driver.send(ExecutorAdded(
    exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
}
```
launchExecutor方法向WorkerEndpoint发送LaunchExecutor的消息，通知Worker根据上面创建的ExecutorDesc启动Executor。然后又向应用ClientEndpoint发送ExecutorAdded消息。
小结：Master收到ClientEndpoint的RegisterApplication消息后，在可用的worker上分配Executors。
## Worker启动Executor
Worker收到LaunchExecutor消息进行处理：
```scala
case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
  ……
    val manager = new ExecutorRunner(
      appId,
      execId,
      appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
      cores_,
      memory_,
      self,
      workerId,
      host,
      webUi.boundPort,
      publicAddress,
      sparkHome,
      executorDir,
      workerUri,
      conf,
      appLocalDirs, ExecutorState.LOADING)
    executors(appId + "/" + execId) = manager
    manager.start()
    coresUsed += cores_
    memoryUsed += memory_
    sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
    ……
  }
```
创建ExecutorRunner对象，ExecutorRunner类负责管理executor进程的执行。调用start方法：
```scala
private[worker] def start() {
  workerThread = new Thread("ExecutorRunner for " + fullId) {
    override def run() { fetchAndRunExecutor() }
  }
  workerThread.start()
  // Shutdown hook that kills actors on shutdown.
  shutdownHook = ShutdownHookManager.addShutdownHook { () =>
    killProcess(Some("Worker shutting down")) }
}
```
ExecutorRunner.start()方法创建一个线程workerThread，并启动，线程执行fetchAndRunExecutor方法：
```scala
private def fetchAndRunExecutor() {
    ……
    // Launch the process
    val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
      memory, sparkHome.getAbsolutePath, substituteVariables)
    val command = builder.command()
    ……
    process = builder.start()
    ……
}
```
fetchAndRunExecutor通过CommandUtils.buildProcessBuilder方法创建ProcessBuilder对象，ProcessBuilder启动进程执行command。命令在CommandUtils.buildProcessBuilder方法中构造，具体为 ` /path/java  -Xms**M -Xmx**M 一些java option参数 mainClass arguments`
mainClass=appDesc.command=org.apache.spark.executor.CoarseGrainedExecutorBackend。

## Executor后台启动
CoarseGrainedExecutorBackend类表示Executor后台，伴生对象的main方法：
```scala
def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
  ……
  run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
}
```
解析参数，然后调用run方法：
```scala
private def run(
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    appId: String,
    workerUrl: Option[String],
    userClassPath: Seq[URL]) {
  ……
  env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
    env.rpcEnv, driverUrl, executorId, sparkHostPort, cores, userClassPath, env))
  ……
}
```
run方法中执行了env.rpcEnv.setupEndpoint，在AkkaRpcEnv中建立名字为“Executor”的RpcEndpoint CoarseGrainedExecutorBackend对象。RpcEndpoint的生命周期是：constructor -> onStart -> receive* -> onStop。因此对象构造完后，首先执行onStart方法，CoarseGrainedExecutorBackend.onStart()方法：
```scala
override def onStart() {
  logInfo("Connecting to driver: " + driverUrl)
  rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
    // This is a very fast action so we can use "ThreadUtils.sameThread"
    driver = Some(ref)
    ref.ask[RegisteredExecutor.type](
      RegisterExecutor(executorId, self, hostPort, cores, extractLogUrls))
  }(ThreadUtils.sameThread).onComplete {
    // This is a very fast action so we can use "ThreadUtils.sameThread"
    case Success(msg) => Utils.tryLogNonFatalError {
      Option(self).foreach(_.send(msg)) // msg must be RegisteredExecutor
    }
    case Failure(e) => {
      logError(s"Cannot register with driver: $driverUrl", e)
      System.exit(1)
    }
  }(ThreadUtils.sameThread)
}
```
在onStart方法中，根据driverUrl获取DriverEndpointRef，然后向DriverEndpoint发送RegisterExecutor消息。
## Driver在空闲资源上启动任务
DriverEndpoint收到RegisterExecutor消息后处理：
```scala
case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
  Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
  if (executorDataMap.contains(executorId)) {
    context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
  } else {
    logInfo("Registered executor: " + executorRef + " with ID " + executorId)
    addressToExecutorId(executorRef.address) = executorId
    totalCoreCount.addAndGet(cores)
    totalRegisteredExecutors.addAndGet(1)
    val (host, _) = Utils.parseHostPort(hostPort)
    val data = new ExecutorData(executorRef, executorRef.address, host, cores, cores, logUrls)
    // This must be synchronized because variables mutated
    // in this block are read when requesting executors
    CoarseGrainedSchedulerBackend.this.synchronized {
      executorDataMap.put(executorId, data)
      if (numPendingExecutors > 0) {
        numPendingExecutors -= 1
        logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
      }
    }
    // Note: some tests expect the reply to come after we put the executor in the map
    context.reply(RegisteredExecutor)
    listenerBus.post(
      SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
    makeOffers()
  }
```
首先检查是否已经注册过，否则创建ExecutorData对象，添加到executorDataMap中，回复RegisteredExecutor消息，最后调用makeOffers()方法。
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
makeOffers方法调用launchTasks在空闲的资源上启动任务。


