title: ApplicationMaster启动过程(二)
date: 2016-05-26 08:38:02
categories: Hadoop 
tags: [Hadoop, Yarn, ApplicationMaster]
---
本章接前一章继续分析ResourceManager启动ApplicationMaster的过程。
<!-- more -->
## RMAppImpl当前信息保存日志
上一章分析到RMAppEventType.START事件触发的状态转移为RMAppNewlySavingTransition，其transition方法执行app.rmContext.getStateStore().storeNewApplication(app)。app是RMAppImpl对象，app.rmContext是RMContextImpl对象，app.rmContext.getStateStore()得到ResourceManager初始化服务时设置的RMStateStore服务，RMStateStore#storeNewApplication方法：
```java
public synchronized void storeNewApplication(RMApp app) {
    ApplicationSubmissionContext context = app.getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationState appState =new ApplicationState(app.getSubmitTime(), app.getStartTime(), context, app.getUser());
    dispatcher.getEventHandler().handle(new RMStateStoreAppEvent(appState));
}
```
方法创建RMStateStoreAppEvent事件放入AsyncDipatcher的事件队列中，RMStateStore服务初始化时为RMStateStoreAppEvent事件注册了事件处理器ForwardingEventHandler：
```java
protected void serviceInit(Configuration conf) throws Exception{
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.register(RMStateStoreEventType.class, new ForwardingEventHandler());
    dispatcher.setDrainEventsOnStop();
    initInternal(conf);
}
```
ForwardingEventHandler#handle方法：
```java
public void handle(RMStateStoreEvent event) {
      handleStoreEvent(event);
}
```
handleStoreEvent方法：
```java
protected void handleStoreEvent(RMStateStoreEvent event) {
    try {
      this.stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.error("Can't handle this event at current state", e);
    }
}
```
RMStateStore内部也维护了一个状态机，描述RMStateStore事件触发的状态转移，RMStateStoreAppEvent的事件类型为RMStateStoreEventType.STORE_APP，状态转移为StoreAppTransition对象，StoreAppTransition#transition方法：
```java
public void transition(RMStateStore store, RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreAppEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      ApplicationState appState = ((RMStateStoreAppEvent) event).getAppState();
      ApplicationId appId = appState.getAppId();
      ApplicationStateData appStateData = ApplicationStateData
          .newInstance(appState);
      LOG.info("Storing info for app: " + appId);
      try {
      	//保存应用的State数据
        store.storeApplicationStateInternal(appId, appStateData);
        //向RMAppImpl发送RMAppEvent.APP_NEW_SAVED消息
        store.notifyApplication(new RMAppEvent(appId,
               RMAppEventType.APP_NEW_SAVED));
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        store.notifyStoreOperationFailed(e);
      }
    };
}
```
RMStateStore#notifyApplication方法：
```java
private void notifyApplication(RMAppEvent event) {
    rmDispatcher.getEventHandler().handle(event);
}
```
rmDispatcher是ResourceManager中创建的AsyncDispatcher对象，向其事件队列中添加RMAppEvent对象，事件类型RMAppEventType.APP_NEW_SAVED触发状态转移AddApplicationToSchedulerTransition，RMAppImpl状态由RMAppState.NEW_SAVING转移到RMAppState.SUBMITTED。AddApplicationToSchedulerTransition#transition方法：
```java
private static final class AddApplicationToSchedulerTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.handler.handle(new AppAddedSchedulerEvent(app.applicationId,
        app.submissionContext.getQueue(), app.user,
        app.submissionContext.getReservationID()));
    }
}
```
## 添加应用到调度队列
方法向事件队列中添加AppAddedSchedulerEvent事件，AppAddedSchedulerEvent继承SchedulerEvent，中央异步事件派发器AsyncDispatcher中注册的SchedulerEvent处理器为SchedulerEventDispatcher对象，因此AsyncDispatcher将SchedulerEvent派发给SchedulerEventDispatcher处理，SchedulerEventDispatcher#handle方法：
```java
public void handle(SchedulerEvent event) {
    try {
        int qSize = eventQueue.size();
        if (qSize !=0 && qSize %1000 == 0) {
          LOG.info("Size of scheduler event-queue is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          LOG.info("Very low remaining capacity on scheduler event queue: "
              + remCapacity);
        }
        this.eventQueue.put(event);
    } catch (InterruptedException e) {
        LOG.info("Interrupted. Trying to exit gracefully.");
    }
}
```
SchedulerEventDispatcher内部也维护了一个事件队列，handle方法将事件放入其事件队列中。SchedulerEventDispatcher作为一个服务，其内部有个专门的事件处理线程，不断的处理队列中的事件：
```java
private final class EventProcessor implements Runnable {
    @Override
    public void run() {
        SchedulerEvent event;
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return; // TODO: Kill RM.
          }

          try {
          	//处理事件
            scheduler.handle(event);
          } catch (Throwable t) {
            // An error occurred, but we are shutting down anyway.
            // If it was an InterruptedException, the very act of 
            // shutdown could have caused it and is probably harmless.
            if (stopped) {
              LOG.warn("Exception during shutdown: ", t);
              break;
            }
            LOG.fatal("Error in handling event type " + event.getType()
                + " to the scheduler", t);
            if (shouldExitOnError
                && !ShutdownHookManager.get().isShutdownInProgress()) {
              LOG.info("Exiting, bbye..");
              System.exit(-1);
            }
          }
        }
    }
}
```
事件交给scheduler处理，scheduler是在ResourceManager初始化服务时创建的：
```java
// Initialize the scheduler
scheduler = createScheduler();
scheduler.setRMContext(rmContext);
addIfService(scheduler);
rmContext.setScheduler(scheduler);

schedulerDispatcher = createSchedulerEventDispatcher();
addIfService(schedulerDispatcher);
rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);
```
createScheduler方法：
```java
protected ResourceScheduler createScheduler() {
	//获取配置的调度器类，默认为FairScheduler
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
      	//通过反射创建调度器类的实例
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
}
```
FairScheduler#handle方法：
```java
public void handle(SchedulerEvent event) {
    switch (event.getType()) {
    ……
    case APP_ADDED:
      if (!(event instanceof AppAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      //添加应用
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser(),
        appAddedEvent.getIsAppRecovering());
      break;
    ……
    }
}
```
AppAddedSchedulerEvent事件的事件类型为SchedulerEventType.APP_ADDED，调用addApplication方法：
```java
protected synchronized void addApplication(ApplicationId applicationId,
      String queueName, String user, boolean isAppRecovering) {
	//检查是否为应用指定提交的队列名
    if (queueName == null || queueName.isEmpty()) {
      String message = "Reject application " + applicationId +
              " submitted by user " + user + " with an empty queue name.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }
    //队列名不合法
    if (queueName.startsWith(".") || queueName.endsWith(".")) {
      String message = "Reject application " + applicationId
          + " submitted by user " + user + " with an illegal queue name "
          + queueName + ". "
          + "The queue name cannot start/end with period.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }
    //将应用添加到队列
    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    FSLeafQueue queue = assignToQueue(rmApp, queueName, user);
    if (queue == null) {
      return;
    }

    // Enforce ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);

    if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      String msg = "User " + userUgi.getUserName() +
              " cannot submit applications to queue " + queue.getName();
      LOG.info(msg);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, msg));
      return;
    }
  
    SchedulerApplication<FSAppAttempt> application =
        new SchedulerApplication<FSAppAttempt>(queue, user);
    applications.put(applicationId, application);
    queue.getMetrics().submitApp(user);

    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName + ", currently num of applications: "
        + applications.size());
    if (isAppRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
      }
    } else {
      //通知RMAppImpl APP_ACCEPTED
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
}
```
## 启动RMAppAttempt
RMAppImpl接收到RMAppEventType.APP_ACCEPTED事件，状态由RMAppState.SUBMITTED变成RMAPPState.ACCEPTED，状态转移StartAppAttemptTransition#transition方法：
```java
private static final class StartAppAttemptTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      //创建并启动新的Attempt
      app.createAndStartNewAttempt(false);
    };
}
```
RMAppImpl#createAndStartNewAttempt方法：
```java
private void createAndStartNewAttempt(boolean transferStateFromPreviousAttempt) {
	//创建RMAppAttempt
    createNewAttempt();
    handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(),
      transferStateFromPreviousAttempt));
}
```
createNewAttempt方法：
```java
private void createNewAttempt() {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);
    RMAppAttempt attempt =
        new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,
          submissionContext, conf,
          // The newly created attempt maybe last attempt if (number of
          // previously failed attempts(which should not include Preempted,
          // hardware error and NM resync) + 1) equal to the max-attempt
          // limit.
          maxAppAttempts == (getNumFailedAppAttempts() + 1), amReq);
    attempts.put(appAttemptId, attempt);
    currentAttempt = attempt;
}
```
方法创建了RMAppAttemptImpl对象，然后由中央异步事件分发器处理RMAppStartAttemptEvent事件，RMAppStartAttemptEvent继承RMAppAttemptEvent，RMAppAttemptEventType事件类型注册的具体事件分发器为ApplicationAttemptEventDispatcher，ApplicationAttemptEventDispatcher#handle方法：
```java
public void handle(RMAppAttemptEvent event) {
    ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
    ApplicationId appAttemptId = appAttemptID.getApplicationId();
    RMApp rmApp = this.rmContext.getRMApps().get(appAttemptId);
    if (rmApp != null) {
    	//获取RMAppAttemptImpl对象
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptID);
        if (rmAppAttempt != null) {
          try {
          	//RMAppAttemptImpl处理事件
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
        }
    }
}
```
RMAppAttemptImpl#handle方法：
```java
 public void handle(RMAppAttemptEvent event) {
    this.writeLock.lock();
    try {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      LOG.debug("Processing event for " + appAttemptID + " of type "
          + event.getType());
      final RMAppAttemptState oldState = getAppAttemptState();
      try {
        //执行事件触发的状态转移
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getAppAttemptState()) {
        LOG.info(appAttemptID + " State change from " + oldState + " to "
            + getAppAttemptState());
      }
    } finally {
      this.writeLock.unlock();
    }
}
```
RMAppAttemptImpl代表应用一次启动尝试，其内部有状态机维护该过程中的状态转移，事件RMAppStartAttemptEvent对应的事件类型为RMAppAttemptEventType.START，触发的状态转移为AttemptStartedTransition，状态机由初始状态RMAppAttemptState.NEW变化为RMAppAttemptState.SUBMITTED，AttemptStartedTransition#transition方法：
```java
private static final class AttemptStartedTransition extends BaseTransition {
	@Override
    public void transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
	  boolean transferStateFromPreviousAttempt = false;
      if (event instanceof RMAppStartAttemptEvent) {
        transferStateFromPreviousAttempt =((RMAppStartAttemptEvent) event).getTransferStateFromPreviousAttempt();
      }
      appAttempt.startTime = System.currentTimeMillis();

      // Register with the ApplicationMasterService
      appAttempt.masterService.registerAppAttempt(appAttempt.applicationAttemptId);

      if (UserGroupInformation.isSecurityEnabled()) {
            appAttempt.rmContext.getClientToAMTokenSecretManager()
        appAttempt.clientTokenMasterKey =
              .createMasterKey(appAttempt.applicationAttemptId);
      }
      //增加应用尝试到调度器并通知调度器
      appAttempt.eventHandler.handle(new AppAttemptAddedSchedulerEvent(
        appAttempt.applicationAttemptId, transferStateFromPreviousAttempt));
    }
}
```
方法创建了事件AppAttemptAddedSchedulerEvent，继承SchedulerEvent，由中央异步事件分发器分发给SchedulerEventDispatcher处理，SchedulerEventDispatcher的事件处理线程调用scheduler处理事件，AppAttemptAddedSchedulerEvent事件类型为SchedulerEventType.APP_ATTEMPT_ADDED，FairSchduler#handle方法：
```java
public void handle(SchedulerEvent event) {
    switch (event.getType()) {
    ……
    case APP_ATTEMPT_ADDED:
      if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      //添加一个新的应用尝试到调度器
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
      break;
    ……
    }
}
```
addApplicationAttempt方法：
```java
protected synchronized void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
	//从应用Map里面取出应用
    SchedulerApplication<FSAppAttempt> application = applications.get(applicationAttemptId.getApplicationId());
    //获取应用的用户
    String user = application.getUser();
    //获取应用的队列
    FSLeafQueue queue = (FSLeafQueue) application.getQueue();
    //创建一个Fair Scheduler视角的应用尝试对象
    FSAppAttempt attempt = new FSAppAttempt(this, applicationAttemptId, user,
            queue, new ActiveUsersManager(getRootQueueMetrics()),rmContext);
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(application
          .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(attempt);

    boolean runnable = maxRunningEnforcer.canAppBeRunnable(queue, user);
    //向队列中添加一个应用尝试
    queue.addApp(attempt, runnable);
    if (runnable) {
      maxRunningEnforcer.trackRunnableApp(attempt);
    } else {
      maxRunningEnforcer.trackNonRunnableApp(attempt);
    }
    
    queue.getMetrics().submitAppAttempt(user);

    LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user: " + user);

    if (isAttemptRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationAttemptId
            + " is recovering. Skipping notifying ATTEMPT_ADDED");
      }
    } else {
      //派发RMAppAttemptEventType.ATTEMPT_ADDED事件
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
    }
}
```
RMAppAttemptEvent事件由ApplicationAttemptEventDispatcher派发处理，在其handle方法中交给RMAppAttemptImpl对象处理，RMAppAttemptEventType.ATTEMPT_ADDED触发RMAppAttemptImpl的状态机由RMAppAttemptState.SUBMITTE状态变化为
为RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING或RMAppAttemptState.SCHEDULED，状态转移类为ScheduleTransition：
```java
public static final class ScheduleTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      ApplicationSubmissionContext subCtx = appAttempt.submissionContext;
      if (!subCtx.getUnmanagedAM()) {
        //为RMAppAttemptImpl设置属性
        appAttempt.amReq.setNumContainers(1);
        appAttempt.amReq.setPriority(AM_CONTAINER_PRIORITY);
        appAttempt.amReq.setResourceName(ResourceRequest.ANY);
        appAttempt.amReq.setRelaxLocality(true);
        
        //分配Container给AM
        Allocation amContainerAllocation =
            appAttempt.scheduler.allocate(appAttempt.applicationAttemptId,
                Collections.singletonList(appAttempt.amReq),
                EMPTY_CONTAINER_RELEASE_LIST, null, null);
        if (amContainerAllocation != null
            && amContainerAllocation.getContainers() != null) {
          assert (amContainerAllocation.getContainers().size() == 0);
        }
        return RMAppAttemptState.SCHEDULED;
      } else {
        // save state and then go to LAUNCHED state
        appAttempt.storeAttempt();
        return RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING;
      }
    }
}
```
RMAppAttemptImpl的scheduler是FairScheduler类，通过FairScheduler#allocate方法给应用尝试分配Container资源：
```java
public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {

    // Make sure this application exists
    FSAppAttempt application = getSchedulerApp(appAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + appAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, new DominantResourceCalculator(),
        clusterResource, minimumAllocation, getMaximumResourceCapability(),
        incrAllocation);

    // Record container allocation start time
    application.recordContainerRequestTime(getClock().getTime());

    // Set amResource for this app
    if (!application.getUnmanagedAM() && ask.size() == 1
        && application.getLiveContainers().isEmpty()) {
      application.setAMResource(ask.get(0).getCapability());
    }

    // 释放containers
    releaseContainers(release, application);

    synchronized (application) {
      if (!ask.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update" +
              " applicationAttemptId=" + appAttemptId +
              " application=" + application.getApplicationId());
        }
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        application.showRequests();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: post-update" +
            " applicationAttemptId=" + appAttemptId +
            " #ask=" + ask.size() +
            " reservation= " + application.getCurrentReservation());

        LOG.debug("Preempting " + application.getPreemptionContainers().size()
            + " container(s)");
      }

      Set<ContainerId> preemptionContainerIds = new HashSet<ContainerId>();
      for (RMContainer container : application.getPreemptionContainers()) {
        preemptionContainerIds.add(container.getContainerId());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      //获取运行ApplicationMaster的container资源
      ContainersAndNMTokensAllocation allocation =
          application.pullNewlyAllocatedContainersAndNMTokens();

      // Record container allocation time
      if (!(allocation.getContainerList().isEmpty())) {
        application.recordContainerAllocationTime(getClock().getTime());
      }

      return new Allocation(allocation.getContainerList(),
        application.getHeadroom(), preemptionContainerIds, null, null,
        allocation.getNMTokenList());
    }
}
```
调用SchedulerApplicationAttempt#pullNewlyAllocatedContainersAndNMTokens:
```java
public synchronized ContainersAndNMTokensAllocation
      pullNewlyAllocatedContainersAndNMTokens() {
    List<Container> returnContainerList =
        new ArrayList<Container>(newlyAllocatedContainers.size());
    List<NMToken> nmTokens = new ArrayList<NMToken>();
    for (Iterator<RMContainer> i = newlyAllocatedContainers.iterator(); i
      .hasNext();) {
      RMContainer rmContainer = i.next();
      Container container = rmContainer.getContainer();
      try {
        // create container token and NMToken altogether.
        container.setContainerToken(rmContext.getContainerTokenSecretManager()
          .createContainerToken(container.getId(), container.getNodeId(),
            getUser(), container.getResource(), container.getPriority(),
            rmContainer.getCreationTime(), this.logAggregationContext));
        NMToken nmToken =
            rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
              getApplicationAttemptId(), container);
        if (nmToken != null) {
          nmTokens.add(nmToken);
        }
      } catch (IllegalArgumentException e) {
        // DNS might be down, skip returning this container.
        LOG.error("Error trying to assign container token and NM token to" +
            " an allocated container " + container.getId(), e);
        continue;
      }
      returnContainerList.add(container);
      i.remove();
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
        RMContainerEventType.ACQUIRED));
    }
    return new ContainersAndNMTokensAllocation(returnContainerList, nmTokens);
}
```

