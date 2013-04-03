/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.yarn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.google.common.collect.Maps;
import java.security.PrivilegedAction;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords
  .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
  .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The YARN Application Master for Giraph is launched when the GiraphYarnClient
 * successfully requests an execution container from the Resource Manager. The
 * Application Master is provided by Giraph to manage all requests for resources
 * (worker nodes, memory, jar files, job configuration metadata, etc.) that
 * Giraph will need to perform the job. When Giraph runs in a non-YARN context,
 * the role of the Application Master is played by Hadoop when it launches our
 * GraphMappers (worker/master task nodes) to run the job.
 */
public class GiraphApplicationMaster {
  /** Logger */
  private static final Logger LOG =
    Logger.getLogger(GiraphApplicationMaster.class);
  /** Exit code for YARN containers that were manually killed/aborted */
  private static final int YARN_ABORT_EXIT_STATUS = -100;
  /** Exit code for successfully run YARN containers */
  private static final int YARN_SUCCESS_EXIT_STATUS = 0;
  /** millis to sleep between heartbeats during long loops */
  private static final int SLEEP_BETWEEN_HEARTBEATS_MSECS = 900;
  /** A reusable map of resources already in HDFS for each task to copy-to-local
   * env and use to launch each GiraphYarnTask. */
  private static Map<String, LocalResource> LOCAL_RESOURCES;
  /** Initialize the Configuration class with the resource file exported by
   * the YarnClient. We will need to export this resource to the tasks also.
   * Construct the HEARTBEAT to use to ping the RM about job progress/health.
   */
  static {
    // pick up new conf XML file and populate it with stuff exported from client
    Configuration.addDefaultResource(GiraphConstants.GIRAPH_YARN_CONF_FILE);
  }

  /** Handle to AppMaster's RPC connection to YARN and the RM. */
  private final AMRMProtocol resourceManager;
  /** bootstrap handle to YARN RPC service */
  private final YarnRPC rpc;
  /** GiraphApplicationMaster's application attempt id */
  private final ApplicationAttemptId appAttemptId;
  /** GiraphApplicationMaster container id. Leave me here, I'm very useful */
  private final ContainerId containerId;
  /** number of containers Giraph needs (conf.getMaxWorkers() + 1 master) */
  private final int containersToLaunch;
  /** MB of JVM heap per Giraph task container */
  private final int heapPerContainer;
  /** Giraph configuration for this job, transported here by YARN framework */
  private final ImmutableClassesGiraphConfiguration giraphConf;
  /** Completed Containers Counter */
  private final AtomicInteger completedCount;
  /** Failed Containers Counter */
  private final AtomicInteger failedCount;
  /** Number of containers requested (hopefully '-w' from our conf) */
  private final AtomicInteger allocatedCount;
  /** Number of successfully completed containers in this job run. */
  private final AtomicInteger successfulCount;
  /** the ACK #'s for AllocateRequests + heartbeats == last response # */
  private AtomicInteger lastResponseId;
  /** Executor to attempt asynchronous launches of Giraph containers */
  private ExecutorService executor;
  /** YARN progress is a <code>float</code> between 0.0f and 1.0f */
  private float progress;
  /** An empty resource request with which to send heartbeats + progress */
  private AllocateRequest heartbeat;

  /**
   * Construct the GiraphAppMaster, populate fields using env vars
   * set up by YARN framework in this execution container.
   * @param cId the ContainerId
   * @param aId the ApplicationAttemptId
   */
  protected GiraphApplicationMaster(ContainerId cId, ApplicationAttemptId aId)
    throws IOException {
    containerId = cId; // future good stuff will need me to operate.
    appAttemptId = aId;
    progress = 0.0f;
    lastResponseId = new AtomicInteger(0);
    giraphConf =
      new ImmutableClassesGiraphConfiguration(new GiraphConfiguration());
    completedCount = new AtomicInteger(0);
    failedCount = new AtomicInteger(0);
    allocatedCount = new AtomicInteger(0);
    successfulCount = new AtomicInteger(0);
    rpc = YarnRPC.create(giraphConf);
    resourceManager = getHandleToRm();
    containersToLaunch = giraphConf.getMaxWorkers() + 1;
    executor = Executors.newFixedThreadPool(containersToLaunch);
    heapPerContainer = giraphConf.getYarnTaskHeapMb();
  }

  /**
   * Coordinates all requests for Giraph's worker/master task containers, and
   * manages application liveness heartbeat, completion status, teardown, etc.
   */
  private void run() {
    // register Application Master with the YARN Resource Manager so we can
    // begin requesting resources. The response contains useful cluster info.
    try {
      resourceManager.registerApplicationMaster(getRegisterAppMasterRequest());
    } catch (IOException ioe) {
      throw new IllegalStateException(
        "GiraphApplicationMaster failed to register with RM.", ioe);
    }

    try {
      // make the request only ONCE; only request more on container failure etc.
      AMResponse amResponse = sendAllocationRequest();
      logClusterResources(amResponse);
      // loop here, waiting for TOTAL # REQUESTED containers to be available
      // and launch them piecemeal they are reported to us in heartbeat pings.
      launchContainersAsynchronously(amResponse);
      // wait for the containers to finish & tally success/fails
      awaitJobCompletion(); // all launched tasks are done before complete call
    } finally {
      // if we get here w/o problems, the executor is already long finished.
      if (null != executor && !executor.isTerminated()) {
        executor.shutdownNow(); // force kill, especially if got here by throw
      }
      // When the application completes, it should send a "finish request" to RM
      try {
        resourceManager.finishApplicationMaster(buildFinishAppMasterRequest());
      } catch (YarnRemoteException yre) {
        LOG.error("GiraphApplicationMaster failed to un-register with RM", yre);
      }
      if (null != rpc) {
        rpc.stopProxy(resourceManager, giraphConf);
      }
    }
  }

  /**
   * Reports the cluster resources in the AM response to our initial ask.
   * @param amResponse the AM response from YARN.
   */
  private void logClusterResources(final AMResponse amResponse) {
    // Check what the current available resources in the cluster are
    Resource availableResources = amResponse.getAvailableResources();
    LOG.info("Initial Giraph resource request for " + containersToLaunch +
      " containers has been submitted. " +
      "The RM reports cluster headroom is: " + availableResources);
  }

  /**
   * Utility to build the final "job run is finished" request to the RM.
   * @return the finish app master request, to send to the RM.
   */
  private FinishApplicationMasterRequest buildFinishAppMasterRequest() {
    LOG.info("Application completed. Signalling finish to RM");
    FinishApplicationMasterRequest finishRequest =
      Records.newRecord(FinishApplicationMasterRequest.class);
    finishRequest.setAppAttemptId(appAttemptId);
    FinalApplicationStatus appStatus;
    String appMessage = "Container Diagnostics: " +
      " allocated=" + allocatedCount.get() +
      ", completed=" + completedCount.get() +
      ", succeeded=" + successfulCount.get() +
      ", failed=" + failedCount.get();
    if (successfulCount.get() == containersToLaunch) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
    }
    finishRequest.setDiagnostics(appMessage);
    finishRequest.setFinishApplicationStatus(appStatus);
    return finishRequest;
  }

  /**
   * Loop and check the status of the containers until all are finished,
   * logging how each container meets its end: success, error, or abort.
   */
  private void awaitJobCompletion() {
    List<ContainerStatus> completedContainers;
    do {
      try {
        Thread.sleep(SLEEP_BETWEEN_HEARTBEATS_MSECS);
      } catch (InterruptedException ignored) {
        final int notFinished = containersToLaunch - completedCount.get();
        LOG.info("GiraphApplicationMaster interrupted from sleep while " +
          " waiting for " + notFinished + "containers to finish job.");
      }
      updateProgress();
      completedContainers =
          sendHeartbeat().getAMResponse().getCompletedContainersStatuses();
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Got container status for containerID= " +
          containerStatus.getContainerId() +
          ", state=" + containerStatus.getState() +
          ", exitStatus=" + containerStatus.getExitStatus() +
          ", diagnostics=" + containerStatus.getDiagnostics());
        switch (containerStatus.getExitStatus()) {
        case YARN_SUCCESS_EXIT_STATUS:
          successfulCount.incrementAndGet();
          break;
        case YARN_ABORT_EXIT_STATUS:
          break; // not success or fail
        default:
          failedCount.incrementAndGet();
          break;
        }
        completedCount.incrementAndGet();
      } // end completion check loop
    } while (completedCount.get() < containersToLaunch);
  }

  /** Update the progress value for our next heartbeat (allocate request) */
  private void updateProgress() {
    // set progress to "half done + ratio of completed containers so far"
    final float ratio = completedCount.get() / (float) containersToLaunch;
    progress = 0.5f + ratio / 2.0f;
  }

  /**
   * Loop while checking container request status, adding each new bundle of
   * containers allocated to our executor to launch (run Giraph BSP task) the
   * job on each. Giraph's full resource request was sent ONCE, but these
   * containers will become available in groups, over a period of time.
   * @param amResponse metadata about our AllocateRequest's results.
   */
  private void launchContainersAsynchronously(AMResponse amResponse) {
    List<Container> allocatedContainers;
    do {
      // get fresh report on # alloc'd containers, sleep between checks
      if (null == amResponse) {
        amResponse = sendHeartbeat().getAMResponse();
      }
      allocatedContainers = amResponse.getAllocatedContainers();
      allocatedCount.addAndGet(allocatedContainers.size());
      LOG.info("Waiting for task containers: " + allocatedCount.get() +
        " allocated out of " + containersToLaunch + " required.");
      startContainerLaunchingThreads(allocatedContainers);
      amResponse = null;
      try {
        Thread.sleep(SLEEP_BETWEEN_HEARTBEATS_MSECS);
      } catch (InterruptedException ignored) {
        LOG.info("launchContainerAsynchronously() raised InterruptedException");
      }
    } while (containersToLaunch > allocatedCount.get());
  }

  /**
   * For each container successfully allocated, attempt to set up and launch
   * a Giraph worker/master task.
   * @param allocatedContainers the containers we have currently allocated.
   */
  private void startContainerLaunchingThreads(final List<Container>
    allocatedContainers) {
    progress = allocatedCount.get() / (2.0f * containersToLaunch);
    int placeholder = 0;
    for (Container allocatedContainer : allocatedContainers) {
      LOG.info("Launching shell command on a new container." +
        ", containerId=" + allocatedContainer.getId() +
        ", containerNode=" + allocatedContainer.getNodeId().getHost() +
        ":" + allocatedContainer.getNodeId().getPort() +
        ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() +
        ", containerState=" + allocatedContainer.getState() +
        ", containerResourceMemory=" +
        allocatedContainer.getResource().getMemory());
      // Launch and start the container on a separate thread to keep the main
      // thread unblocked as all containers may not be allocated at one go.
      LaunchContainerRunnable launchThread =
        new LaunchContainerRunnable(allocatedContainer, heapPerContainer);
      executor.execute(launchThread);
    }
  }

  /**
   * Sends heartbeat messages that include progress amounts. These are in the
   * form of a YARN AllocateRequest object that asks for 0 resources.
   * @return the AllocateResponse, which we may or may not need.
   */
  private AllocateResponse sendHeartbeat() {
    heartbeat.setProgress(progress);
    heartbeat.setResponseId(lastResponseId.incrementAndGet());
    AllocateResponse allocateResponse = null;
    try {
      allocateResponse = resourceManager.allocate(heartbeat);
      final int responseId = allocateResponse.getAMResponse().getResponseId();
      if (responseId != lastResponseId.get()) {
        lastResponseId.set(responseId);
      }
      checkForRebootFlag(allocateResponse.getAMResponse());
      return allocateResponse;
    } catch (YarnRemoteException yre) {
      throw new IllegalStateException("sendHeartbeat() failed with " +
        "YarnRemoteException: ", yre);
    }
  }

  /**
   * Compose and send the allocation request for our Giraph BSP worker/master
   * compute nodes. Right now the requested containers are identical, mirroring
   * Giraph's behavior when running on Hadoop MRv1. Giraph could use YARN
   * to set fine-grained capability to each container, including host choice.
   * @return The AM resource descriptor with our container allocations.
   */
  private AMResponse sendAllocationRequest() {
    AllocateRequest allocRequest = Records.newRecord(AllocateRequest.class);
    try {
      List<ResourceRequest> containerList = buildResourceRequests();
      allocRequest.addAllAsks(containerList);
      List<ContainerId> releasedContainers = Lists.newArrayListWithCapacity(0);
      allocRequest.setResponseId(lastResponseId.get());
      allocRequest.setApplicationAttemptId(appAttemptId);
      allocRequest.addAllReleases(releasedContainers);
      allocRequest.setProgress(progress);
      AllocateResponse allocResponse = resourceManager.allocate(allocRequest);
      AMResponse amResponse = allocResponse.getAMResponse();
      if (amResponse.getResponseId() != lastResponseId.get()) {
        lastResponseId.set(amResponse.getResponseId());
      }
      checkForRebootFlag(amResponse);
      // now, make THIS our new HEARTBEAT object, but with ZERO new requests!
      initHeartbeatRequestObject(allocRequest);
      return amResponse;
    } catch (YarnRemoteException yre) {
      throw new IllegalStateException("Giraph Application Master could not " +
        "successfully allocate the specified containers from the RM.", yre);
    }
  }

  /**
   * If the YARN RM gets way out of sync with our App Master, its time to
   * fail the job/restart. This should trigger the job end and cleanup.
   * @param amResponse RPC response from YARN RM to check for reboot flag.
   */
  private void checkForRebootFlag(AMResponse amResponse) {
    if (amResponse.getReboot()) {
      LOG.error("AMResponse: " + amResponse + " raised YARN REBOOT FLAG!");
      throw new RuntimeException("AMResponse " + amResponse +
        " signaled GiraphApplicationMaster with REBOOT FLAG. Failing job.");
    }
  }


  /**
   * Reuses the initial container request (switched to "0 asks" so no new allocs
   * occur) and sends all heartbeats using that request object.
   * @param allocRequest the allocation request object to use as heartbeat.
   */
  private void initHeartbeatRequestObject(AllocateRequest allocRequest) {
    allocRequest.clearAsks();
    allocRequest.addAllAsks(Lists.<ResourceRequest>newArrayListWithCapacity(0));
    heartbeat = allocRequest;
  }

  /**
   * Utility to construct the ResourceRequest for our resource ask: all the
   * Giraph containers we need, and their memory/priority requirements.
   * @return a list of ResourceRequests to send (just one, for Giraph tasks)
   */
  private List<ResourceRequest> buildResourceRequests() {
    // set up resource request for our Giraph BSP application
    ResourceRequest resourceRequest = Records.newRecord(ResourceRequest.class);
    resourceRequest.setHostName("*"); // hand pick our worker locality someday
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(GiraphConstants.GIRAPH_YARN_PRIORITY);
    resourceRequest.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setVirtualCores(1); // new YARN API, won't work version < 2.0.3
    capability.setMemory(heapPerContainer);
    resourceRequest.setCapability(capability);
    resourceRequest.setNumContainers(containersToLaunch);
    return ImmutableList.of(resourceRequest);
  }

  /**
   * Obtain handle to RPC connection to Resource Manager.
   * @return the AMRMProtocol handle to YARN RPC.
   */
  private AMRMProtocol getHandleToRm() {
    YarnConfiguration yarnConf = new YarnConfiguration(giraphConf);
    final InetSocketAddress rmAddress = yarnConf.getSocketAddr(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation currentUser;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException ioe) {
        throw new IllegalStateException("Could not obtain UGI for user.", ioe);
      }
      String tokenURLEncodedStr = System.getenv(
        ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();
      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException ioe) {
        throw new IllegalStateException("Could not decode token from URL", ioe);
      }
      SecurityUtil.setTokenService(token, rmAddress);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is: " + token);
      }
      currentUser.addToken(token);
      return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
        @Override
        public AMRMProtocol run() {
          return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            rmAddress, giraphConf);
        }
      });
    } else { // non-secure
      return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
        rmAddress, yarnConf);
    }
  }

  /**
   * Get the request to register this Application Master with the RM.
   * @return the populated AM request.
   */
  private RegisterApplicationMasterRequest getRegisterAppMasterRequest() {
    RegisterApplicationMasterRequest appMasterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setApplicationAttemptId(appAttemptId);
    try {
      appMasterRequest.setHost(InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException uhe) {
      throw new IllegalStateException(
        "Cannot resolve GiraphApplicationMaster's local hostname.", uhe);
    }
    // useful for a Giraph WebUI or whatever: play with these
    // appMasterRequest.setRpcPort(appMasterRpcPort);
    // appMasterRequest.setTrackingUrl(appMasterTrackingUrl);
    return appMasterRequest;
  }

  /**
   * Lazily compose the map of jar and file names to LocalResource records for
   * inclusion in GiraphYarnTask container requests. Can re-use the same map
   * as Giraph tasks need identical HDFS-based resources (jars etc.) to run.
   * @return the resource map for a ContainerLaunchContext
   */
  private Map<String, LocalResource> getTaskResourceMap() {
    // Set the local resources: just send the copies already in HDFS
    if (null == LOCAL_RESOURCES) {
      LOCAL_RESOURCES = Maps.newHashMap();
      try {
        // if you have to update the giraphConf for export to tasks, do it now
        updateGiraphConfForExport();
        YarnUtils.addFsResourcesToMap(LOCAL_RESOURCES, giraphConf,
          appAttemptId.getApplicationId());
      } catch (IOException ioe) {
        // fail fast, this container will never launch.
        throw new IllegalStateException("Could not configure the container" +
          "launch context for GiraphYarnTasks.", ioe);
      }
    }
    // else, return the prepopulated copy to reuse for each GiraphYarkTask
    return LOCAL_RESOURCES;
  }

  /**
   * If you're going to make ANY CHANGES to your local GiraphConfiguration
   * while running the GiraphApplicationMaster, put them here.
   * This method replaces the current XML file GiraphConfiguration
   * stored in HDFS with the copy you have modified locally in-memory.
   */
  private void updateGiraphConfForExport()
    throws IOException {
    // Giraph expects this MapReduce stuff
    giraphConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
      appAttemptId.getAttemptId());
    // now republish the giraph-conf.xml in HDFS
    YarnUtils.exportGiraphConfiguration(giraphConf,
      appAttemptId.getApplicationId());
  }

  /**
   * Thread to connect to the {@link ContainerManager} and launch the container
   * that will house one of our Giraph worker (or master) tasks.
   */
  private class LaunchContainerRunnable implements Runnable {
    /** Allocated container */
    private Container container;
    /** Handle to communicate with ContainerManager */
    private ContainerManager containerManager;
    /** Heap memory in MB to allocate for this JVM in the launched container */
    private final int heapSize;

    /**
     * Constructor.
     * @param newGiraphTaskContainer Allocated container
     * @param heapMb the <code>-Xmx</code> setting for each launched task.
     */
    public LaunchContainerRunnable(final Container newGiraphTaskContainer,
      final int heapMb) {
      this.container = newGiraphTaskContainer;
      this.heapSize = heapMb;
    }

    /**
     * Helper function to connect to ContainerManager, which resides on the
     * same compute node as this Giraph task's container. The CM starts tasks.
     */
    private void connectToCM() {
      LOG.debug("Connecting to CM for containerid=" + container.getId());
      String cmIpPortStr = container.getNodeId().getHost() + ":" +
        container.getNodeId().getPort();
      InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
      LOG.info("Connecting to CM at " + cmIpPortStr);
      this.containerManager = (ContainerManager)
        rpc.getProxy(ContainerManager.class, cmAddress, giraphConf);
    }

    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
      // Connect to ContainerManager
      connectToCM();
      // configure the launcher for the Giraph task it will host
      StartContainerRequest startReq =
        Records.newRecord(StartContainerRequest.class);
      startReq.setContainerLaunchContext(buildContainerLaunchContext());
      // request CM to start this container as spec'd in ContainerLaunchContext
      try {
        containerManager.startContainer(startReq);
      } catch (YarnRemoteException yre) {
        LOG.error("StartContainerRequest failed for containerId=" +
                    container.getId(), yre);
      }
    }

    /**
     * Boilerplate to set up the ContainerLaunchContext to tell the Container
     * Manager how to launch our Giraph task in the execution container we have
     * already allocated.
     * @return a populated ContainerLaunchContext object.
     */
    private ContainerLaunchContext buildContainerLaunchContext() {
      LOG.info("Setting up container launch container for containerid=" +
        container.getId());
      ContainerLaunchContext launchContext = Records
        .newRecord(ContainerLaunchContext.class);
      launchContext.setContainerId(container.getId());
      launchContext.setResource(container.getResource());
      // args inject the CLASSPATH, heap MB, and TaskAttemptID for launched task
      final List<String> commands = generateShellExecCommand();
      launchContext.setCommands(commands);
      // add user information to the job
      String jobUserName = "ERROR_UNKNOWN_USER";
      UserGroupInformation ugi = null;
      try {
        ugi = UserGroupInformation.getCurrentUser();
        jobUserName = ugi.getUserName();
      } catch (IOException ioe) {
        jobUserName =
          System.getenv(ApplicationConstants.Environment.USER.name());
      }
      launchContext.setUser(jobUserName);
      LOG.info("Setting username in ContainerLaunchContext to: " + jobUserName);
      // Set the environment variables to inject into remote task's container
      buildEnvironment(launchContext);
      // Set the local resources: just send the copies already in HDFS
      launchContext.setLocalResources(getTaskResourceMap());
      return launchContext;
    }

    /**
     * Generates our command line string used to launch our Giraph tasks.
     * @return the BASH shell commands to launch the job.
     */
    private List<String> generateShellExecCommand() {
      return ImmutableList.of("java " +
        "-Xmx" + heapSize + "M " +
        "-Xms" + heapSize + "M " +
        "-cp .:${CLASSPATH} " +
        "org.apache.giraph.yarn.GiraphYarnTask " +
        appAttemptId.getApplicationId().getClusterTimestamp() + " " +
        appAttemptId.getApplicationId().getId() + " " +
        container.getId().getId() + " " +
        appAttemptId.getAttemptId() + " " +
        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        "/task-" + container.getId().getId() + "-stdout.log " +
        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        "/task-" + container.getId().getId() + "-stderr.log "
      );
    }

    /**
     * Utility to populate the environment vars we wish to inject into the new
     * containter's env when the Giraph BSP task is executed.
     * @param launchContext the launch context which will set our environment
     *                      vars in the app master's execution container.
     */
    private void buildEnvironment(final ContainerLaunchContext launchContext) {
      Map<String, String> classPathForEnv = Maps.<String, String>newHashMap();
      // pick up the local classpath so when we instantiate a Configuration
      // remotely, we also get the "mapred-site.xml" and "yarn-site.xml"
      YarnUtils.addLocalClasspathToEnv(classPathForEnv, giraphConf);
      // set this map of env vars into the launch context.
      launchContext.setEnvironment(classPathForEnv);
    }
  }

  /**
   * Application entry point
   * @param args command-line args (set by GiraphYarnClient, if any)
   */
  public static void main(final String[] args) {
    String containerIdString =
        System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      throw new IllegalArgumentException("ContainerId not found in env vars.");
    }
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
    try {
      GiraphApplicationMaster giraphAppMaster =
        new GiraphApplicationMaster(containerId, appAttemptId);
      giraphAppMaster.run();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Throwable t) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("GiraphApplicationMaster caught a " +
                  "top-level exception in main.", t);
      System.exit(2);
    }
    System.exit(0);
  }
}
