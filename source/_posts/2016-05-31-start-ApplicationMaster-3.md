title: ApplicationMaster启动过程(三)
date: 2016-05-31 10:20:08
categories: Hadoop 
tags: [Hadoop, Yarn, ApplicationMaster]
---
本章接前一章继续分析ResourceManager启动ApplicationMaster的过程。
<!-- more -->
## RMContainer的创建过程
ResourceManager收到NodeManager通过心跳机制汇报的信息后，会触发一个NODE_UPDATE事件，可能有新的Container资源释放，因此会触发资源分配，然后调度器从队列中为应用分配Container资源，创建RMContainerImpl对象，FSAppAttempt#allocate方法：
```java
synchronized public RMContainer allocate(NodeType type, FSSchedulerNode node,
      Priority priority, ResourceRequest request, Container container) {
    // Update allowed locality level
    NodeType allowed = allowedLocalityLevel.get(priority);
    if (allowed != null) {
      if (allowed.equals(NodeType.OFF_SWITCH) &&
          (type.equals(NodeType.NODE_LOCAL) ||
              type.equals(NodeType.RACK_LOCAL))) {
        this.resetAllowedLocalityLevel(priority, type);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL) &&
          type.equals(NodeType.NODE_LOCAL)) {
        this.resetAllowedLocalityLevel(priority, type);
      }
    }

    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // 创建RMContainerImpl对象
    RMContainer rmContainer = new RMContainerImpl(container, 
        getApplicationAttemptId(), node.getNodeID(),
        appSchedulingInfo.getUser(), rmContext);

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    

    // Update consumption and track allocations
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
    Resources.addTo(currentConsumption, container.getResource());

    // Update resource requests related to "request" and store in RMContainer
    ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

    // 通知RMContainerImpl启动
    rmContainer.handle(new RMContainerEvent(container.getId(), RMContainerEventType.START));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId=" 
          + container.getId().getApplicationAttemptId() 
          + " container=" + container.getId() + " host="
          + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId());
    
    return rmContainer;
}
```
RMContainerImpl处理RMContainerEventType.START事件，RMContainerImpl状态机从状态RMContainerState.NEW转移成RMContainerState.ALLOCATED，触发状态转移ContainerStartedTransition：
```java
private static final class ContainerStartedTransition extends BaseTransition {
    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      container.eventHandler.handle(new RMAppAttemptContainerAllocatedEvent(
          container.appAttemptId));
    }
}
```
向RMAppAttemptImpl发送RMAppAttemptEventType.CONTAINER_ALLOCATED事件，RMAppAttemptImpl收到后，状态从RMAppAttemptState.SCHEDULED转移为RMAppAttemptState.ALLOCATED_SAVING，触发状态转移AMContainerAllocatedTransition：
```java
private static final class AMContainerAllocatedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Acquire the AM container from the scheduler.
      Allocation amContainerAllocation =
          appAttempt.scheduler.allocate(appAttempt.applicationAttemptId,
            EMPTY_CONTAINER_REQUEST_LIST, EMPTY_CONTAINER_RELEASE_LIST, null, null);
      
      if (amContainerAllocation.getContainers().size() == 0) {
        appAttempt.retryFetchingAMContainer(appAttempt);
        return RMAppAttemptState.SCHEDULED;
      }

      // Set the masterContainer
      appAttempt.setMasterContainer(amContainerAllocation.getContainers()
          .get(0));
      RMContainerImpl rmMasterContainer = (RMContainerImpl)appAttempt.scheduler
          .getRMContainer(appAttempt.getMasterContainer().getId());
      rmMasterContainer.setAMContainer(true);
      
      appAttempt.rmContext.getNMTokenSecretManager()
        .clearNodeSetForAttempt(appAttempt.applicationAttemptId);
      appAttempt.getSubmissionContext().setResource(
        appAttempt.getMasterContainer().getResource());
      appAttempt.storeAttempt();
      return RMAppAttemptState.ALLOCATED_SAVING;
    }
}
```
