title: ApplicationMaster启动过程(一)
date: 2016-05-24 14:06:29
categories: Hadoop 
tags: [Hadoop, Yarn, ApplicationMaster]
---
本章分析ResourceManager启动ApplicationMaster的过程。
<!-- more -->
## ClientRMService处理提交应用的RPC请求
ResorceManager中的ClientRMService是实现了ApplicationClientProtocol协议的RPC Server，客户端调用RPC函数ApplicationClientProtocol#submitApplication提交应用程序。ClientRMService.submitApplication方法：
```java
public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    ApplicationId applicationId = submissionContext.getApplicationId();

    // ApplicationSubmissionContext needs to be validated for safety - only
    // those fields that are independent of the RM's configuration will be
    // checked here, those that are dependent on RM configuration are validated
    // in RMAppManager.

    String user = null;
    try {
      // Safety
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException ie) {
      LOG.warn("Unable to get the current user.", ie);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
          ie.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    // Check whether app has already been put into rmContext,
    // If it is, simply return the response
    if (rmContext.getRMApps().get(applicationId) != null) {
      LOG.info("This is an earlier submitted application: " + applicationId);
      return SubmitApplicationResponse.newInstance();
    }

    if (submissionContext.getQueue() == null) {
      submissionContext.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }
    if (submissionContext.getApplicationName() == null) {
      submissionContext.setApplicationName(
          YarnConfiguration.DEFAULT_APPLICATION_NAME);
    }
    if (submissionContext.getApplicationType() == null) {
      submissionContext
        .setApplicationType(YarnConfiguration.DEFAULT_APPLICATION_TYPE);
    } else {
      if (submissionContext.getApplicationType().length() > YarnConfiguration.APPLICATION_TYPE_LENGTH) {
        submissionContext.setApplicationType(submissionContext
          .getApplicationType().substring(0,
            YarnConfiguration.APPLICATION_TYPE_LENGTH));
      }
    }

    try {
      // 调用RMAppManager直接提交应用
      rmAppManager.submitApplication(submissionContext,
          System.currentTimeMillis(), user);

      LOG.info("Application with id " + applicationId.getId() + 
          " submitted by user " + user);
      RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
          "ClientRMService", applicationId);
    } catch (YarnException e) {
      LOG.info("Exception in submitting application with id " +
          applicationId.getId(), e);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
          e.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId);
      throw e;
    }

    SubmitApplicationResponse response = recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
    return response;
}
```
调用RMAppManager.submitApplication方法提交应用：
```java
protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();
    //创建RMAppImpl对象维护应用的运行状态
    RMAppImpl application =
        createAndPopulateNewRMApp(submissionContext, submitTime, user);
    ApplicationId appId = submissionContext.getApplicationId();

    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        this.rmContext.getDelegationTokenRenewer().addApplicationAsync(appId,
            parseCredentials(submissionContext),
            submissionContext.getCancelTokensWhenComplete(),
            application.getUser());
      } catch (Exception e) {
        LOG.warn("Unable to parse credentials.", e);
        // Sending APP_REJECTED is fine, since we assume that the
        // RMApp is in NEW state and thus we haven't yet informed the
        // scheduler about the existence of the application
        assert application.getState() == RMAppState.NEW;
        this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, e.getMessage()));
        throw RPCUtil.getRemoteException(e);
      }
    } else {
      // Dispatcher is not yet started at this time, so these START events
      // enqueued should be guaranteed to be first processed when dispatcher
      // gets started.
      this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.START));
    }
}
```
## 启动ApplicationMaster事件处理
ResourceManager采用事件驱动的机制，通过事件来触发服务的执行。上面的方法中this.rmContext是RMContextImpl对象，调用getDispatcher()方法得到AsyncDispatcher对象，负责分发事件，内部维护了一个事件队列eventQueue和一个事件类型和事件处理器的哈希表eventDispatchers。AsyncDispatcher对象调用getEventHandler()方法得到GenericEventHandler对象，该对象的handler方法将RMAppEventType.START事件放入事件队列。
```java
public EventHandler getEventHandler() {
    if (handlerInstance == null) {
      handlerInstance = new GenericEventHandler();
    }
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize !=0 && qSize %1000 == 0) {
        LOG.info("Size of event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue: "
            + remCapacity);
      }
      try {
      	//事件加入事件队列
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        throw new YarnRuntimeException(e);
      }
    };
}
```
AsyncDispatcher也是一个服务，启动后创建一个事件处理线程eventHandlingThread执行createThread()：
```java
Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          drained = eventQueue.isEmpty();
          // blockNewEvents is only set when dispatcher is draining to stop,
          // adding this check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }
          Event event;
          try {
            event = eventQueue.take(); //取事件
          } catch(InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher thread interrupted", ie);
            }
            return;
          }
          if (event != null) {
            dispatch(event); //分发事件
          }
        }
      }
    };
}
```
线程不断的从事件队列中取出事件，然后调用dispatch(event)派发事件：
```java
protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "."
          + event.toString());
    }

    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try{
      //根据事件类型得到对应的事件处理器
      EventHandler handler = eventDispatchers.get(type);
      if(handler != null) {
        handler.handle(event);
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.fatal("Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      if (exitOnDispatchException
          && (ShutdownHookManager.get().isShutdownInProgress()) == false
          && stopped == false) {
        Thread shutDownThread = new Thread(createShutDownThread());
        shutDownThread.setName("AsyncDispatcher ShutDown handler");
        shutDownThread.start();
      }
    }
}
```
在ResourceManager初始化服务时，AsyncDispatcher注册了多种事件类型和对应的处理器。其中RMAppEventType事件类型对应的事件处理器是ApplicationEventDispathcher：
```java
// Register event handler for RmAppEvents
rmDispatcher.register(RMAppEventType.class, new ApplicationEventDispatcher(rmContext));
```
AsyncDispatcher.register方法：
```java
public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>)
    eventDispatchers.get(eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      //事件类型和事件处理器添加的hashMap中
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)){
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler
      = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
}
```
因此对于RMAppEventType.START事件，调用ApplicationEventDispatcher.handle方法处理：
```java
public void handle(RMAppEvent event) {
	//获取应用ID
    ApplicationId appID = event.getApplicationId();
    //根据应用ID取得应用的RMAppImpl对象
    RMApp rmApp = this.rmContext.getRMApps().get(appID);
    if (rmApp != null) {
        try {
          //由RMAppImpl对象处理事件
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
    }
}
```
RMAppImpl.handle方法：
```java
public void handle(RMAppEvent event) {
    this.writeLock.lock();
    try {
      ApplicationId appID = event.getApplicationId();
      LOG.debug("Processing event for " + appID + " of type "
          + event.getType());
      final RMAppState oldState = getState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getState()) {
        LOG.info(appID + " State change from " + oldState + " to "
            + getState());
      }
    } finally {
      this.writeLock.unlock();
    }
}
```
## RMAppImpl状态机
RMAppImpl内部维护了一个状态机，表示应用的状态，事件会触发状态的转移。RMAppImpl中有个StateMachineFactory对象，通过StateMachineFactory#addTransition方法，根据前状态，后状态，触发状态转移的事件和转移生成一个新的StateMachineFactory
```java
public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState,
                        EVENTTYPE eventType,
                        SingleArcTransition<OPERAND, EVENT> hook){
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
        (this, new ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
           (preState, eventType, new SingleInternalArc(postState, hook)));
}
```
方法返回一个新的StateMachineFactory对象
```java
  private StateMachineFactory
      (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
    //默认初始状态等于旧对象的默认初始状态
    this.defaultInitialState = that.defaultInitialState;
    //转移链表节点，节点的next指向旧对象的转移链表节点
    this.transitionsListNode 
        = new TransitionsListNode(t, that.transitionsListNode);
    this.optimized = false;
    this.stateMachineTable = null;
  }
```
在RMAppImpl中StateMachineFactory对象通过addTransition方法将全部的状态转移构造成对应的StateMachineFacotry对象，它们的状态转移列表节点transitionListNode连接转移链表，最后调用StateMachineFactory#installTopology方法：
```java
public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          installTopology() {
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, true);
}
```
方法还是构造StateMachineFactory对象，构造方法：
```java
private StateMachineFactory
      (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       boolean optimized) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode = that.transitionsListNode;
    this.optimized = optimized;
    if (optimized) {
   	  //创建状态机表
      makeStateMachineTable();
    } else {
      stateMachineTable = null;
    }
}
```
在该构造方法中，参数optimized=true，调用makeStateMachineTable()函数：
```java
private void makeStateMachineTable() {
    Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack =
      new Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>();

    Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
      prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

    prototype.put(defaultInitialState, null);

    // I use EnumMap here because it'll be faster and denser.  I would
    //  expect most of the states to have at least one transition.
    stateMachineTable
       = new EnumMap<STATE, Map<EVENTTYPE,
                           Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);

    for (TransitionsListNode cursor = transitionsListNode;
         cursor != null;
         cursor = cursor.next) {
      stack.push(cursor.transition);
    }

    while (!stack.isEmpty()) {
      stack.pop().apply(this);
    }
}
```
方法将状态转移链表节点的transition成员依次压栈，然后再依次出栈，transition是ApplicableSingleOrMultipleTransition对象，调用其apply()方法：
```java
public void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
	//从stateMachineTable中取状态为preState的transitionMap
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
        = subject.stateMachineTable.get(preState);
    //如果为空，创建transitionMap并添加到stateMachineTable
    if (transitionMap == null) {
        // I use HashMap here because I would expect most EVENTTYPE's to not
        //  apply out of a particular state, so FSM sizes would be 
        //  quadratic if I use EnumMap's here as I do at the top level.
        transitionMap = new HashMap<EVENTTYPE,
          Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
        subject.stateMachineTable.put(preState, transitionMap);
    }
    //将eventType和transition添加到transitionMap
    transitionMap.put(eventType, transition);
}
```
方法将每个事件类型(eventType)和对应的状态转移(transition)关联成哈希表transitionMap，然后将前状态(preState)和对应的transitionMap添加到stateMachineTable哈希表。stateMachineTable将RMAppImpl状态机的全部状态转移进行了索引。

## 状态转移处理
第二节分析到RMAppEventType.START事件经过一系列流转，最终由RMAppImpl处理，在RMAppImpl#handle方法中执行 
**this.stateMachine.doTransition(event.getType(), event);**
开始事件触发的状态转移。首先看一下RMAppImpl中的stateMachine成员：

```java
this.stateMachine = stateMachineFactory.make(this);
```
stateMachine成员由RMAppImpl中的stateMachineFactory成员调用make方法构造：
```java
public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
    return new InternalStateMachine(operand, defaultInitialState);
}
```
make方法返回InternalStateMachine对象，因此InternalStateMachine#doTransition方法：
```java
public synchronized STATE doTransition(EVENTTYPE eventType, EVENT event)
        throws InvalidStateTransitonException  {
    currentState = StateMachineFactory.this.doTransition
        (operand, currentState, eventType, event);
    return currentState;
}
```
方法中调用StateMachineFactory#doTransition方法：
```java
private STATE doTransition(OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
      throws InvalidStateTransitonException {
    //查询stateMachineTable，取得oldState对应的transitionMap
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
      = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      //查询transitionMap，取得eventType对应的transition
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
          = transitionMap.get(eventType);
      if (transition != null) {
      	//执行状态转移
        return transition.doTransition(operand, oldState, event, eventType);
      }
    }
    throw new InvalidStateTransitonException(oldState, eventType);
}
```
查询stateMachineTable获取事件对应的状态转移，对于preState=RMAppState.NEW，eventType=RMAppEventType.START，hook=RMAppNewlySavingTransition()，transition是SingleInternalArc对象，SingleInternalArc#doTransition方法：
```java
public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
    if (hook != null) {
        hook.transition(operand, event);
    }
    return postState;
}
```
调用RMAppNewlySavingTransition#transition方法：
```java
public void transition(RMAppImpl app, RMAppEvent event) {
    LOG.info("Storing application with id " + app.applicationId);
    app.rmContext.getStateStore().storeNewApplication(app);
}
```
RMAppImpl状态从RMAppState.NEW转移到RMAppState.NEW_SAVING。