title: Hadoop提交作业到Yarn
date: 2016-05-20 14:53:21
categories: Hadoop 
tags: [Hadoop, Yarn]
---
本章分析Hadoop向Yarn提交作业的过程。
<!-- more -->
## 提交作业
Hadoop通过RunJar类运行jar作业。RunJar类的main方法中通反射调用我们编写MR程序中main方法。以WordCount为例：
```java
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if(otherArgs.length < 2) {
        System.err.println("Usage: wordcoount <in> [<in>...] <out>");
        System.exit(2);
    }
    //创建一个job
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    //设置mapper,combiner,reducer类
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    //设置输入输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //设置输入输出目录
    for(int i = 0; i < otherArgs.length - 1; ++i) {
        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
    //进入作业提交
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
```
通过job.waitForCompletion方法开始提交作业：
```java
public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
    if (state == JobState.DEFINE) {
      //提交作业
      submit(); 
    }
    if (verbose) {
      monitorAndPrintJob();
    } else {
      // get the completion poll interval from the client.
      int completionPollIntervalMillis = 
        Job.getCompletionPollInterval(cluster.getConf());
      while (!isComplete()) {
        try {
          Thread.sleep(completionPollIntervalMillis);
        } catch (InterruptedException ie) {
        }
      }
    }
    return isSuccessful();
  }
```
调用submit()函数：
```java
public void submit() throws IOException, InterruptedException, ClassNotFoundException {
    ensureState(JobState.DEFINE);
    setUseNewAPI();
    //连接集群
    connect(); 
    final JobSubmitter submitter = 
        getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
    status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
      public JobStatus run() throws IOException, InterruptedException, 
      ClassNotFoundException {
        return submitter.submitJobInternal(Job.this, cluster);
      }
    });
    state = JobState.RUNNING;
    LOG.info("The url to track the job: " + getTrackingURL());
}
```
调用connect()函数创建集群实例：
```java
private synchronized void connect() throws IOException, InterruptedException, ClassNotFoundException {
    if (cluster == null) {
      cluster = 
        ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
                   public Cluster run()
                          throws IOException, InterruptedException, 
                                 ClassNotFoundException {
                     return new Cluster(getConfiguration());
                   }
                 });
    }
}
```
Cluster类在构造方法中调用initialize方法初始化：
```java
private void initialize(InetSocketAddress jobTrackAddr, Configuration conf) throws IOException {
    synchronized (frameworkLoader) {
      for (ClientProtocolProvider provider : frameworkLoader) {
        LOG.debug("Trying ClientProtocolProvider : "
            + provider.getClass().getName());
        ClientProtocol clientProtocol = null; 
        try {
          if (jobTrackAddr == null) {
            clientProtocol = provider.create(conf);
          } else {
            clientProtocol = provider.create(jobTrackAddr, conf);
          }

          if (clientProtocol != null) {
            clientProtocolProvider = provider;
            client = clientProtocol;
            LOG.debug("Picked " + provider.getClass().getName()
                + " as the ClientProtocolProvider");
            break;
          }
          else {
            LOG.debug("Cannot pick " + provider.getClass().getName()
                + " as the ClientProtocolProvider - returned null protocol");
          }
        } 
        catch (Exception e) {
          LOG.info("Failed to use " + provider.getClass().getName()
              + " due to error: " + e.getMessage());
        }
      }
    }

    if (null == clientProtocolProvider || null == client) {
      throw new IOException(
          "Cannot initialize Cluster. Please check your configuration for "
              + MRConfig.FRAMEWORK_NAME
              + " and the correspond server addresses.");
    }
}
```
initialize方法通过ClientProtocolProvider创建ClientProtocol，ClientProtocolProvider有两个具体的子类LocalClientProtocolProvider和YarnClientProtocolPrivider，它们的create方法根据参数mapreduce.framework.name创建对应的ClientProtocol。YarnClientProtocolPrivider.create()方法：
```java
public ClientProtocol create(Configuration conf) throws IOException {
    if (MRConfig.YARN_FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
      return new YARNRunner(conf);
    }
    return null;
}
```
当mapreduce.framework.name=yarn时，创建YARNRunner对象返回。返回的YARNRunner对象是cluster对象的client成员。回到Job.submit方法，连接集群后，调用getJobSubmitter函数创建了JobSubmitter对象，然后执行submitter.submitJobInternal(Job.this, cluster)：
```java
JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {

    //validate the jobs output specs 
    checkSpecs(job);

    Configuration conf = job.getConfiguration();
    addMRFrameworkToDistributedCache(conf);

    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
    //configure the command line options correctly on the submitting dfs
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }
    //获取jobId
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    try {
      conf.set(MRJobConfig.USER_NAME,
          UserGroupInformation.getCurrentUser().getShortUserName());
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
          + " as the submit dir");
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
      populateTokenCache(conf, job.getCredentials());

      // generate a secret to authenticate shuffle transfers
      if (TokenCache.getShuffleSecretKey(job.getCredentials()) == null) {
        KeyGenerator keyGen;
        try {
         
          int keyLen = CryptoUtils.isShuffleEncrypted(conf) 
              ? conf.getInt(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS, 
                  MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS)
              : SHUFFLE_KEY_LENGTH;
          keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM);
          keyGen.init(keyLen);
        } catch (NoSuchAlgorithmException e) {
          throw new IOException("Error generating shuffle secret key", e);
        }
        SecretKey shuffleKey = keyGen.generateKey();
        TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(),
            job.getCredentials());
      }
      //上传作业的jar文件和配置的-files, -libjars, -archives文件
      copyAndConfigureFiles(job, submitJobDir);
      
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      
      // 产生InputSplit文件
      LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
      int maps = writeSplits(job, submitJobDir);
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      LOG.info("number of splits:" + maps);

      //设置队列
      String queue = conf.get(MRJobConfig.QUEUE_NAME,
          JobConf.DEFAULT_QUEUE_NAME);
      AccessControlList acl = submitClient.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      if (conf.getBoolean(
          MRJobConfig.JOB_TOKEN_TRACKING_IDS_ENABLED,
          MRJobConfig.DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED)) {
        // Add HDFS tracking ids
        ArrayList<String> trackingIds = new ArrayList<String>();
        for (Token<? extends TokenIdentifier> t :
            job.getCredentials().getAllTokens()) {
          trackingIds.add(t.decodeIdentifier().getTrackingId());
        }
        conf.setStrings(MRJobConfig.JOB_TOKEN_TRACKING_IDS,
            trackingIds.toArray(new String[trackingIds.size()]));
      }

      // Set reservation info if it exists
      ReservationId reservationId = job.getReservationId();
      if (reservationId != null) {
        conf.set(MRJobConfig.RESERVATION_ID, reservationId.toString());
      }

      // Write job file to submit dir
      //将job的配置写入job.xml
      writeConf(conf, submitJobFile);

      printTokens(jobId, job.getCredentials());
      // 实际提交job
      status = submitClient.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (jtFs != null && submitJobDir != null)
          jtFs.delete(submitJobDir, true);

      }
    }
  }
```
总结一下，submitJobInternal做了七件事：
1. 创建存放作业执行中用到的文件的目录；
2. 获取jobID；
3. 上传作业文件到1创建的目录中，包括程序jar包，配置的-files, -libjars, -archives文件；
4. 生成InputSplit文件；
5. 设置作业提交到的运行队列；
6. 生成作业的配置文件job.xml；
7. 提交作业。

YARNRunner.submitJob方法：
```java
public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {
    
    addHistoryToken(ts);
    
    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext =
      createApplicationSubmissionContext(conf, jobSubmitDir, ts);

    //提交给ResourceManager
    try {
      ApplicationId applicationId =
          resMgrDelegate.submitApplication(appContext);

      ApplicationReport appMaster = resMgrDelegate
          .getApplicationReport(applicationId);
      String diagnostics =
          (appMaster == null ?
              "application report is null" : appMaster.getDiagnostics());
      if (appMaster == null
          || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
          || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
        throw new IOException("Failed to run job : " +
            diagnostics);
      }
      return clientCache.getClient(jobId).getJobStatus(jobId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }
```
调用ResourceMgrDelegate.submitApplication方法：
```java
public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    return client.submitApplication(appContext);
  }
```
ResourceMgrDelegate调用成员client:YarnClientImpl.submitApplication方法：
```java
public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    ApplicationId applicationId = appContext.getApplicationId();
    if (applicationId == null) {
      throw new ApplicationIdNotProvidedException(
          "ApplicationId is not provided in ApplicationSubmissionContext");
    }
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);

    // Automatically add the timeline DT into the CLC
    // Only when the security and the timeline service are both enabled
    if (isSecurityEnabled() && timelineServiceEnabled) {
      addTimelineDelegationToken(appContext.getAMContainerSpec());
    }

    //TODO: YARN-1763:Handle RM failovers during the submitApplication call.
    rmClient.submitApplication(request);

    int pollCount = 0;
    long startTime = System.currentTimeMillis();

    while (true) {
      try {
        YarnApplicationState state =
            getApplicationReport(applicationId).getYarnApplicationState();
        if (!state.equals(YarnApplicationState.NEW) &&
            !state.equals(YarnApplicationState.NEW_SAVING)) {
          LOG.info("Submitted application " + applicationId);
          break;
        }

        long elapsedMillis = System.currentTimeMillis() - startTime;
        if (enforceAsyncAPITimeout() &&
            elapsedMillis >= asyncApiPollTimeoutMillis) {
          throw new YarnException("Timed out while waiting for application " +
              applicationId + " to be submitted successfully");
        }

        // Notify the client through the log every 10 poll, in case the client
        // is blocked here too long.
        if (++pollCount % 10 == 0) {
          LOG.info("Application submission is not finished, " +
              "submitted application " + applicationId +
              " is still in " + state);
        }
        try {
          Thread.sleep(submitPollIntervalMillis);
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while waiting for application "
              + applicationId
              + " to be successfully submitted.");
        }
      } catch (ApplicationNotFoundException ex) {
        // FailOver or RM restart happens before RMStateStore saves
        // ApplicationState
        LOG.info("Re-submit application " + applicationId + "with the " +
            "same ApplicationSubmissionContext");
        rmClient.submitApplication(request);
      }
    }

    return applicationId;
  }
```
通过rmClient.submitApplication(request)对RPC Server发起RPC调用。
