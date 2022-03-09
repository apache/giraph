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

import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;

/**
 * The initial launcher for a YARN-based Giraph job. This class attempts to
 * configure and send a request to the ResourceManager for a single
 * application container to host GiraphApplicationMaster. The RPC connection
 * between the RM and GiraphYarnClient is the YARN ApplicationManager.
 */
public class GiraphYarnClient {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphYarnClient.class);
  /** Sleep time between silent progress checks */
  private static final int JOB_STATUS_INTERVAL_MSECS = 800;
  /** Memory (in MB) to allocate for our ApplicationMaster container */
  private static final int YARN_APP_MASTER_MEMORY_MB = 512;

  /** human-readable job name */
  private final String jobName;
  /** Helper configuration from the job */
  private final GiraphConfiguration giraphConf;
  /** ApplicationId object (needed for RPC to ResourceManager) */
  private ApplicationId appId;
  /** # of sleeps between progress reports to client */
  private int reportCounter;
  /** Yarn client object */
  private YarnClient yarnClient;

  /**
   * Constructor. Requires caller to hand us a GiraphConfiguration.
   *
   * @param giraphConf User-defined configuration
   * @param jobName User-defined job name
   */
  public GiraphYarnClient(GiraphConfiguration giraphConf, String jobName)
    throws IOException {
    this.reportCounter = 0;
    this.jobName = jobName;
    this.appId = null; // can't set this until after start()
    this.giraphConf = giraphConf;
    verifyOutputDirDoesNotExist();
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(giraphConf);
  }

  /**
   * Submit a request to the Hadoop YARN cluster's ResourceManager
   * to obtain an application container. This will run our ApplicationMaster,
   * which will in turn request app containers for Giraphs' master and all
   * worker tasks.
   * @param verbose Not implemented yet, to provide compatibility w/GiraphJob
   * @return true if job is successful
   */
  public boolean run(final boolean verbose) throws YarnException, IOException {
    // init our connection to YARN ResourceManager RPC
    LOG.info("Running Client");
    yarnClient.start();
    // request an application id from the RM
 // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse getNewAppResponse = app.
      getNewApplicationResponse();
    checkPerNodeResourcesAvailable(getNewAppResponse);
    // configure our request for an exec container for GiraphApplicationMaster
    ApplicationSubmissionContext appContext = app.
      getApplicationSubmissionContext();
    appId = appContext.getApplicationId();
    //createAppSubmissionContext(appContext);
    appContext.setApplicationId(appId);
    appContext.setApplicationName(jobName);
    LOG.info("Obtained new Application ID: " + appId);
    // sanity check
    applyConfigsForYarnGiraphJob();

    ContainerLaunchContext containerContext = buildContainerLaunchContext();
    appContext.setResource(buildContainerMemory());
    appContext.setAMContainerSpec(containerContext);
    LOG.info("ApplicationSumbissionContext for GiraphApplicationMaster " +
      "launch container is populated.");
    //TODO: priority and queue
    // Set the priority for the application master
    //Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    //pri.setPriority(amPriority);
    //appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    //appContext.setQueue(amQueue);

   // make the request, blow up if fail, loop and report job progress if not
    try {
      LOG.info("Submitting application to ASM");
      // obtain an "updated copy" of the appId for status checks/job kill later
      appId = yarnClient.submitApplication(appContext);
      LOG.info("Got new appId after submission :" + appId);
    } catch (YarnException yre) {
      // TODO
      // Try submitting the same request again
      // app submission failure?
      throw new RuntimeException("submitApplication(appContext) FAILED.", yre);
    }
    LOG.info("GiraphApplicationMaster container request was submitted to " +
      "ResourceManager for job: " + jobName);
    return awaitGiraphJobCompletion();
  }

  /**
   * Without Hadoop MR to check for us, make sure the output dir doesn't exist!
   */
  private void verifyOutputDirDoesNotExist() {
    Path outDir = null;
    try {
      FileSystem fs = FileSystem.get(giraphConf);
      String errorMsg = "__ERROR_NO_OUTPUT_DIR_SET__";
      outDir =
        new Path(fs.getHomeDirectory(), giraphConf.get(OUTDIR, errorMsg));
      FileStatus outStatus = fs.getFileStatus(outDir);
      if (outStatus.isDirectory() || outStatus.isFile() ||
        outStatus.isSymlink()) {
        throw new IllegalStateException("Path " + outDir + " already exists.");
      }
    } catch (IOException ioe) {
      LOG.info("Final output path is: " + outDir);
    }
  }

  /**
   * Configuration settings we need to customize for a Giraph on YARN
   * job. We need to call this EARLY in the job, before the GiraphConfiguration
   * is exported to HDFS for localization in each task container.
   */
  private void applyConfigsForYarnGiraphJob() {
    GiraphConstants.IS_PURE_YARN_JOB.set(giraphConf, true);
    GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf, true);
    giraphConf.set("mapred.job.id", "giraph_yarn_" + appId); // ZK app base path
  }

  /**
   * Utility to make sure we have the cluster resources we need to run this
   * job. If they are not available, we should die here before too much setup.
   * @param cluster the GetNewApplicationResponse from the YARN RM.
   */
  private void checkPerNodeResourcesAvailable(
    final GetNewApplicationResponse cluster) throws YarnException, IOException {
    // are there enough containers to go around for our Giraph job?
    List<NodeReport> nodes = null;
    long totalAvailable = 0;
    try {
      nodes = yarnClient.getNodeReports(NodeState.RUNNING);
    } catch (YarnException yre) {
      throw new RuntimeException("GiraphYarnClient could not connect with " +
        "the YARN ResourceManager to determine the number of available " +
        "application containers.", yre);
    }
    for (NodeReport node : nodes) {
      LOG.info("Got node report from ASM for" +
        ", nodeId=" + node.getNodeId() +
        ", nodeAddress " + node.getHttpAddress() +
        ", nodeRackName " + node.getRackName() +
        ", nodeNumContainers " + node.getNumContainers());
      totalAvailable += node.getCapability().getMemory();
    }
    // 1 master + all workers in -w command line arg
    final int workers = giraphConf.getMaxWorkers() + 1;
    checkAndAdjustPerTaskHeapSize(cluster);
    final long totalAsk =
      giraphConf.getYarnTaskHeapMb() * workers;
    if (totalAsk > totalAvailable) {
      throw new IllegalStateException("Giraph's estimated cluster heap " +
        totalAsk + "MB ask is greater than the current available cluster " +
        "heap of " + totalAvailable + "MB. Aborting Job.");
    }
  }

  /**
   * Adjust the user-supplied <code>-yh</code> and <code>-w</code>
   * settings if they are too small or large for the current cluster,
   * and re-record the new settings in the GiraphConfiguration for export.
   * @param gnar the GetNewAppResponse from the YARN ResourceManager.
   */
  private void checkAndAdjustPerTaskHeapSize(
    final GetNewApplicationResponse gnar) {
    // do we have the right heap size on these cluster nodes to run our job?
    //TODO:
    //final int minCapacity = gnar.getMinimumResourceCapability().getMemory();
    final int maxCapacity = gnar.getMaximumResourceCapability().getMemory();
    // make sure heap size is OK for this cluster's available containers
    int giraphMem = giraphConf.getYarnTaskHeapMb();
    if (giraphMem == GiraphConstants.GIRAPH_YARN_TASK_HEAP_MB_DEFAULT) {
      LOG.info("Defaulting per-task heap size to " + giraphMem + "MB.");
    }
    if (giraphMem > maxCapacity) {
      LOG.info("Giraph's request of heap MB per-task is more than the " +
        "maximum; downgrading Giraph to" + maxCapacity + "MB.");
      giraphMem = maxCapacity;
    }
    /*if (giraphMem < minCapacity) { //TODO:
      LOG.info("Giraph's request of heap MB per-task is less than the " +
        "minimum; upgrading Giraph to " + minCapacity + "MB.");
      giraphMem = minCapacity;
    }*/
    giraphConf.setYarnTaskHeapMb(giraphMem); // record any changes made
  }

  /**
   * Kill time for the client, report progress occasionally, and otherwise
   * just sleep and wait for the job to finish. If no AM response, kill the app.
   * @return true if job run is successful.
   */
  private boolean awaitGiraphJobCompletion() throws YarnException, IOException {
    boolean done;
    ApplicationReport report = null;
    try {
      do {
        try {
          Thread.sleep(JOB_STATUS_INTERVAL_MSECS);
        } catch (InterruptedException ir) {
          LOG.info("Progress reporter's sleep was interrupted!", ir);
        }
        report = yarnClient.getApplicationReport(appId);
        done = checkProgress(report);
      } while (!done);
      if (!giraphConf.metricsEnabled()) {
        cleanupJarCache();
      }
    } catch (IOException ex) {
      final String diagnostics = (null == report) ? "" :
        "Diagnostics: " + report.getDiagnostics();
      LOG.error("Fatal fault encountered, failing " + jobName + ". " +
        diagnostics, ex);
      try {
        LOG.error("FORCIBLY KILLING Application from AppMaster.");
        yarnClient.killApplication(appId);
      } catch (YarnException yre) {
        LOG.error("Exception raised in attempt to kill application.", yre);
      }
      return false;
    }
    return printFinalJobReport();
  }

  /**
   * Deletes the HDFS cache in YARN, which replaces DistributedCache of Hadoop.
   * If metrics are enabled this will not get called (so you can examine cache.)
   * @throws IOException if bad things happen.
   */
  private void cleanupJarCache() throws IOException {
    FileSystem fs = FileSystem.get(giraphConf);
    Path baseCacheDir = YarnUtils.getFsCachePath(fs, appId);
    if (fs.exists(baseCacheDir)) {
      LOG.info("Cleaning up HDFS distributed cache directory for Giraph job.");
      fs.delete(baseCacheDir, true); // stuff inside
      fs.delete(baseCacheDir, false); // dir itself
    }
  }

  /**
   * Print final formatted job report for local client that initiated this run.
   * @return true for app success, false for failure.
   */
  private boolean printFinalJobReport() throws YarnException, IOException {
    ApplicationReport report;
    try {
      report = yarnClient.getApplicationReport(appId);
      FinalApplicationStatus finalAppStatus =
        report.getFinalApplicationStatus();
      final long secs =
        (report.getFinishTime() - report.getStartTime()) / 1000L;
      final String time = String.format("%d minutes, %d seconds.",
        secs / 60L, secs % 60L);
      LOG.info("Completed " + jobName + ": " +
        finalAppStatus.name() + ", total running time: " + time);
    } catch (YarnException yre) {
      LOG.error("Exception encountered while attempting to request " +
        "a final job report for " + jobName , yre);
      return false;
    }
    return true;
  }

  /**
   * Compose the ContainerLaunchContext for the Application Master.
   * @return the CLC object populated and configured.
   */
  private ContainerLaunchContext buildContainerLaunchContext()
    throws IOException {
    ContainerLaunchContext appMasterContainer =
      Records.newRecord(ContainerLaunchContext.class);
    appMasterContainer.setEnvironment(buildEnvironment());
    appMasterContainer.setLocalResources(buildLocalResourceMap());
    appMasterContainer.setCommands(buildAppMasterExecCommand());
    //appMasterContainer.setResource(buildContainerMemory());
    //appMasterContainer.setUser(ApplicationConstants.Environment.USER.name());
    setToken(appMasterContainer);
    return appMasterContainer;
  }

  /**
   * Set delegation tokens for AM container
   * @param amContainer AM container
   * @return
   */
  private void setToken(ContainerLaunchContext amContainer) throws IOException {
    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = giraphConf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
          "Can't get Master Kerberos principal for the RM to use as renewer");
      }
      FileSystem fs = FileSystem.get(giraphConf);
      // For now, only getting tokens for the default file-system.
      final Token<?> [] tokens =
        fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }
  }

  /**
   * Assess whether job is already finished/failed and 'done' flag needs to be
   * set, prints progress display for client if all is going well.
   * @param report the application report to assess.
   * @return true if job report indicates the job run is over.
   */
  private boolean checkProgress(final ApplicationReport report) {
    YarnApplicationState jobState = report.getYarnApplicationState();
    if (jobState == YarnApplicationState.FINISHED ||
      jobState == YarnApplicationState.KILLED) {
      return true;
    } else if (jobState == YarnApplicationState.FAILED) {
      LOG.error(jobName + " reports FAILED state, diagnostics show: " +
        report.getDiagnostics());
      return true;
    } else {
      if (reportCounter++ % 5 == 0) {
        displayJobReport(report);
      }
    }
    return false;
  }

  /**
   * Display a formatted summary of the job progress report from the AM.
   * @param report the report to display.
   */
  private void displayJobReport(final ApplicationReport report) {
    if (null == report) {
      throw new IllegalStateException("[*] Latest ApplicationReport for job " +
        jobName + " was not received by the local client.");
    }
    final float elapsed =
      (System.currentTimeMillis() - report.getStartTime()) / 1000.0f;
    LOG.info(jobName + ", Elapsed: " + String.format("%.2f secs", elapsed));
    LOG.info(report.getCurrentApplicationAttemptId() + ", State: " +
      report.getYarnApplicationState().name() + ", Containers used: " +
      report.getApplicationResourceUsageReport().getNumUsedContainers());
  }

  /**
   * Utility to produce the command line to activate the AM from the shell.
   * @return A <code>List<String></code> of shell commands to execute in
   *         the container allocated to us by the RM to host our App Master.
   */
  private List<String> buildAppMasterExecCommand() {
    // 'gam-' prefix is for GiraphApplicationMaster in log file names
    return ImmutableList.of("${JAVA_HOME}/bin/java " +
      "-Xmx" + YARN_APP_MASTER_MEMORY_MB + "M " +
      "-Xms" + YARN_APP_MASTER_MEMORY_MB + "M " + // TODO: REMOVE examples jar!
      //TODO: Make constant
      "-cp .:${CLASSPATH} org.apache.giraph.yarn.GiraphApplicationMaster " +
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/gam-stdout.log " +
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/gam-stderr.log "
    );
  }

  /**
   * Register all local jar files from GiraphConstants.GIRAPH_YARN_LIBJARS
   * in the LocalResources map, copy to HDFS on that same registered path.
   * @param map the LocalResources list to populate.
   */
  private void addLocalJarsToResourceMap(Map<String, LocalResource> map)
    throws IOException {
    Set<String> jars = Sets.newHashSet();
    LOG.info("LIB JARS :" + giraphConf.getYarnLibJars());
    String[] libJars = giraphConf.getYarnLibJars().split(",");
    for (String libJar : libJars) {
      jars.add(libJar);
    }
    FileSystem fs = FileSystem.get(giraphConf);
    Path baseDir = YarnUtils.getFsCachePath(fs, appId);
    for (Path jar : YarnUtils.getLocalFiles(jars)) {
      Path dest = new Path(baseDir, jar.getName());
      LOG.info("Made local resource for :" + jar + " to " +  dest);
      fs.copyFromLocalFile(false, true, jar, dest);
      YarnUtils.addFileToResourceMap(map, fs, dest);
    }
  }

  /**
   * Construct the memory requirements for the AppMaster's container request.
   * @return A Resource that wraps the memory request.
   */
  private Resource buildContainerMemory() {
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(YARN_APP_MASTER_MEMORY_MB); //Configurable thru CLI?
    return capability;
  }

  /**
   * Create the mapping of environment vars that will be visible to the
   * ApplicationMaster in its remote app container.
   * @return a map of environment vars to set up for the AppMaster.
   */
  private Map<String, String> buildEnvironment() {
    Map<String, String> environment =
      Maps.<String, String>newHashMap();
    LOG.info("Set the environment for the application master");
    YarnUtils.addLocalClasspathToEnv(environment, giraphConf);
    //TODO: add the runtime classpath needed for tests to work
    LOG.info("Environment for AM :" + environment);
    return environment;
  }

  /**
   * Create the mapping of files and JARs to send to the GiraphApplicationMaster
   * and from there on to the Giraph tasks.
   * @return the map of jars to local resource paths for transport
   *   to the host container that will run our AppMaster.
   */
  private Map<String, LocalResource> buildLocalResourceMap() {
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master
    //is part of the local resources
    Map<String, LocalResource> localResources =
        Maps.<String, LocalResource>newHashMap();
    LOG.info("buildLocalResourceMap ....");
    try {
      // export the GiraphConfiguration to HDFS for localization to remote tasks
      //Ques: Merge the following two method
      YarnUtils.exportGiraphConfiguration(giraphConf, appId);
      YarnUtils.addGiraphConfToLocalResourceMap(
        giraphConf, appId, localResources);
      // add jars from '-yj' cmd-line arg to resource map for localization
      addLocalJarsToResourceMap(localResources);
      //TODO: log4j?
      return localResources;
    } catch (IOException ioe) {
      throw new IllegalStateException("Failed to build LocalResouce map.", ioe);
    }
  }

}
