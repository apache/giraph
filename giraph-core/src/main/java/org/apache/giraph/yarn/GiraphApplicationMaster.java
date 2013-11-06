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

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords
  .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
//TODO
  /** For status update for clients - yet to be implemented\\
  * Hostname of the container
  */
  private String appMasterHostname = "";
  /** Port on which the app master listens for status updates from clients*/
  private int appMasterRpcPort = 0;
  /** Tracking url to which app master publishes info for clients to monitor*/
  private String appMasterTrackingUrl = "";

  static {
    // pick up new conf XML file and populate it with stuff exported from client
    Configuration.addDefaultResource(GiraphConstants.GIRAPH_YARN_CONF_FILE);
  }

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
  /** Yarn configuration for this job*/
  private final YarnConfiguration yarnConf;
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
  /** buffer tostore all tokens */
  private ByteBuffer allTokens;
  /** Executor to attempt asynchronous launches of Giraph containers */
  private ExecutorService executor;
  /** YARN progress is a <code>float</code> between 0.0f and 1.0f */
  //Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;
  /** Handle to communicate with the Node Manager */
  private NMClientAsync nmClientAsync;
  /** Listen to process the response from the Node Manager */
  private NMCallbackHandler containerListener;
  /** whether all containers finishe */
  private volatile boolean done;

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
    lastResponseId = new AtomicInteger(0);
    giraphConf =
      new ImmutableClassesGiraphConfiguration(new GiraphConfiguration());
    yarnConf = new YarnConfiguration(giraphConf);
    completedCount = new AtomicInteger(0);
    failedCount = new AtomicInteger(0);
    allocatedCount = new AtomicInteger(0);
    successfulCount = new AtomicInteger(0);
    containersToLaunch = giraphConf.getMaxWorkers() + 1;
    executor = Executors.newFixedThreadPool(containersToLaunch);
    heapPerContainer = giraphConf.getYarnTaskHeapMb();
    LOG.info("GiraphAM  for ContainerId " + cId + " ApplicationAttemptId " +
      aId);
  }

  /**
   * Coordinates all requests for Giraph's worker/master task containers, and
   * manages application liveness heartbeat, completion status, teardown, etc.
   * @return success or failure
   */
  private boolean run() throws YarnException, IOException {
    boolean success = false;
    try {
      getAllTokens();
      registerRMCallBackHandler();
      registerNMCallbackHandler();
      registerAMToRM();
      madeAllContainerRequestToRM();
      LOG.info("Wait to finish ..");
      while (!done) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          LOG.error(ex);
          //TODO:
        }
      }
      LOG.info("Done " + done);
    } finally {
      // if we get here w/o problems, the executor is already long finished.
      if (null != executor && !executor.isTerminated()) {
        LOG.info("Forcefully terminating executors with done =:" + done);
        executor.shutdownNow(); // force kill, especially if got here by throw
      }
      success = finish();
    }
    return success;
  }

  /**
   * Call when the application is done
   * @return if all containers succeed
   */
  private boolean finish() {
    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");
    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (failedCount.get() == 0 &&
        completedCount.get() == containersToLaunch) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + containersToLaunch +
        ", completed=" + completedCount.get() +  ", failed=" +
        failedCount.get();
      success = false;
    }
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();
    return success;
  }
  /**
   * Add all containers' request
   * @return
   */
  private void madeAllContainerRequestToRM() {
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    for (int i = 0; i < containersToLaunch; ++i) {
      ContainerRequest containerAsk = setupContainerAskForRM();
      amRMClient.addContainerRequest(containerAsk);
    }
  }

   /**
    * Setup the request that will be sent to the RM for the container ask.
    *
    * @return the setup ResourceRequest to be sent to RM
    */
  private ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(GiraphConstants.GIRAPH_YARN_PRIORITY);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(heapPerContainer);

    ContainerRequest request = new ContainerRequest(capability, null, null,
      pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  /**
   * Populate allTokens with the tokens received
   * @return
   */
  private void getAllTokens() throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser()
        .getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token type :" + token.getKind());
      }
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  /**
   * Register RM callback and start listening
   * @return
   */
  private void registerRMCallBackHandler() {
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000,
      allocListener);
    amRMClient.init(yarnConf);
    amRMClient.start();
  }

  /**
   * Register NM callback and start listening
   * @return
   */
  private void registerNMCallbackHandler() {
    containerListener = new NMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(yarnConf);
    nmClientAsync.start();
  }
  /**
   * Register AM to RM
   * @return AM register response
   */
  private RegisterApplicationMasterResponse registerAMToRM()
    throws YarnException {
    // register Application Master with the YARN Resource Manager so we can
    // begin requesting resources.
    try {
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("SECURITY ENABLED ");
      }
      // TODO: provide actual call back details
      RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(appMasterHostname
        , appMasterRpcPort, appMasterTrackingUrl);
      return response;
    } catch (IOException ioe) {
      throw new IllegalStateException(
        "GiraphApplicationMaster failed to register with RM.", ioe);
    }
  }

  /**
   * For each container successfully allocated, attempt to set up and launch
   * a Giraph worker/master task.
   * @param allocatedContainers the containers we have currently allocated.
   */
  private void startContainerLaunchingThreads(final List<Container>
    allocatedContainers) {
    for (Container allocatedContainer : allocatedContainers) {
      LOG.info("Launching command on a new container." +
        ", containerId=" + allocatedContainer.getId() +
        ", containerNode=" + allocatedContainer.getNodeId().getHost() +
        ":" + allocatedContainer.getNodeId().getPort() +
        ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() +
        ", containerResourceMemory=" +
        allocatedContainer.getResource().getMemory());
      // Launch and start the container on a separate thread to keep the main
      // thread unblocked as all containers may not be allocated at one go.
      LaunchContainerRunnable runnableLaunchContainer =
        new LaunchContainerRunnable(allocatedContainer, containerListener);
      executor.execute(runnableLaunchContainer);
    }
  }

  /**
   * Lazily compose the map of jar and file names to LocalResource records for
   * inclusion in GiraphYarnTask container requests. Can re-use the same map
   * as Giraph tasks need identical HDFS-based resources (jars etc.) to run.
   * @return the resource map for a ContainerLaunchContext
   */
  private synchronized Map<String, LocalResource> getTaskResourceMap() {
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
   * Application entry point
   * @param args command-line args (set by GiraphYarnClient, if any)
   */
  public static void main(final String[] args) {
    boolean result = false;
    LOG.info("Starting GitaphAM ");
    String containerIdString =  System.getenv().get(
      Environment.CONTAINER_ID.name());
    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      throw new IllegalArgumentException("ContainerId not found in env vars.");
    }
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
    try {
      GiraphApplicationMaster giraphAppMaster =
        new GiraphApplicationMaster(containerId, appAttemptId);
      result = giraphAppMaster.run();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Throwable t) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("GiraphApplicationMaster caught a " +
                  "top-level exception in main.", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Giraph Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Giraph Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Thread to connect to the {@link ContainerManager} and launch the container
   * that will house one of our Giraph worker (or master) tasks.
   */
  private class LaunchContainerRunnable implements Runnable {
    /** Allocated container */
    private Container container;
    /** NM listener */
    private NMCallbackHandler containerListener;

    /**
     * Constructor.
     * @param newGiraphTaskContainer Allocated container
     * @param containerListener container listener.
     */
    public LaunchContainerRunnable(final Container newGiraphTaskContainer,
      NMCallbackHandler containerListener) {
      this.container = newGiraphTaskContainer;
      this.containerListener = containerListener;
    }

    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
      // Connect to ContainerManager
      // configure the launcher for the Giraph task it will host
      ContainerLaunchContext ctx = buildContainerLaunchContext();
      // request CM to start this container as spec'd in ContainerLaunchContext
      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
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
      // args inject the CLASSPATH, heap MB, and TaskAttemptID for launched task
      final List<String> commands = generateShellExecCommand();
      LOG.info("Conatain launch Commands :" + commands.get(0));
      launchContext.setCommands(commands);
      // Set up tokens for the container too. We are
      // populating them mainly for NodeManagers to be able to download any
      // files in the distributed file-system. The tokens are otherwise also
      // useful in cases, for e.g., when one is running a
      // "hadoop dfs" like command
      launchContext.setTokens(allTokens.slice());

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
      //launchContext.setUser(jobUserName);
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
        "-Xmx" + heapPerContainer + "M " +
        "-Xms" + heapPerContainer + "M " +
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
   * CallbackHandler to process RM async calls
   */
  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus>
      completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt=" +
        completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Got container status for containerID=" +
          containerStatus.getContainerId() + ", state=" +
          containerStatus.getState() + ", exitStatus=" +
          containerStatus.getExitStatus() + ", diagnostics=" +
          containerStatus.getDiagnostics());
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
      }

      if (completedCount.get() == containersToLaunch) {
        done = true;
        LOG.info("All container compeleted. done = " + done);
      } else {
        LOG.info("After completion of one conatiner. current status is:" +
          " completedCount :" + completedCount.get() +
          " containersToLaunch :" + containersToLaunch +
          " successfulCount :" + successfulCount.get() +
          " failedCount :" + failedCount.get());
      }
    }
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt=" +
          allocatedContainers.size());
      allocatedCount.addAndGet(allocatedContainers.size());
      LOG.info("Total allocated # of container so far : " +
        allocatedCount.get() +
        " allocated out of " + containersToLaunch + " required.");
      startContainerLaunchingThreads(allocatedContainers);
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) completedCount.get() /
          containersToLaunch;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }
  }

  /**
   * CallbackHandler to process NM async calls
   */
  private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    /** List of containers */
    private ConcurrentMap<ContainerId, Container> containers =
          new ConcurrentHashMap<ContainerId, Container>();

    /**
     * Add a container
     * @param containerId id of container
     * @param container container object
     * @return
     */
    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        nmClientAsync.getContainerStatusAsync(containerId,
          container.getNodeId());
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId, t);
      containers.remove(containerId);
    }

    @Override
    public void onGetContainerStatusError(
      ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId, t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }
  }
}
