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

import com.google.common.collect.Sets;
import java.io.FileOutputStream;
import java.util.Set;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.util.StringUtils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Utilities that can only compile with versions of Hadoop that support YARN,
 * so they live here instead of o.a.g.utils package.
 */
public class YarnUtils {
  /** Class Logger */
  private static final Logger LOG = Logger.getLogger(YarnUtils.class);
  /** Default dir on HDFS (or equivalent) where LocalResources are stored */
  private static final String HDFS_RESOURCE_DIR = "giraph_yarn_jar_cache";

  /** Private constructor, this is a utility class only */
  private YarnUtils() { /* no-op */ }

  /**
   * Populates the LocalResources list with the HDFS paths listed in
   * the conf under GiraphConstants.GIRAPH_YARN_LIBJARS, and the
   * GiraphConfiguration for this job. Also adds the Giraph default application
   * jar as determined by GiraphYarnClient.GIRAPH_CLIENT_JAR constant.
   * @param map the LocalResources list to populate.
   * @param giraphConf the configuration to use to select jars to include.
   * @param appId the ApplicationId, naming the the HDFS base dir for job jars.
   */
  public static void addFsResourcesToMap(Map<String, LocalResource> map,
    GiraphConfiguration giraphConf, ApplicationId appId) throws IOException {
    FileSystem fs = FileSystem.get(giraphConf);
    Path baseDir = YarnUtils.getFsCachePath(fs, appId);
    boolean coreJarFound = false;
    for (String fileName : giraphConf.getYarnLibJars().split(",")) {
      if (fileName.length() > 0) {
        Path filePath = new Path(baseDir, fileName);
        LOG.info("Adding " + fileName + " to LocalResources for export.to " +
          filePath);
        if (fileName.contains("giraph-core")) {
          coreJarFound = true;
        }
        addFileToResourceMap(map, fs, filePath);
      }
    }
    if (!coreJarFound) { // OK if you are running giraph-examples-jar-with-deps
      LOG.warn("Job jars (-yj option) didn't include giraph-core.");
    }
    Path confPath = new Path(baseDir, GiraphConstants.GIRAPH_YARN_CONF_FILE);
    addFileToResourceMap(map, fs, confPath);
  }

  /**
   * Utility function to locate local JAR files and other resources
   * recursively in the dirs on the local CLASSPATH. Once all the files
   * named in <code>fileNames</code> are found, we stop and return the results.
   * @param fileNames the file name of the jars, without path information.
   * @return a set of Paths to the jar files requested in fileNames.
   */
  public static Set<Path> getLocalFiles(final Set<String> fileNames) {
    Set<Path> jarPaths = Sets.newHashSet();
    String classPath = ".:" + System.getenv("HADOOP_HOME");
    if (classPath.length() > 2) {
      classPath += ":";
    }
    classPath += System.getenv("CLASSPATH");
    for (String baseDir : classPath.split(":")) {
      LOG.info("Class path name " + baseDir);
      if (baseDir.length() > 0) {
        // lose the globbing chars that will fail in File#listFiles
        final int lastFileSep = baseDir.lastIndexOf("/");
        if (lastFileSep > 0) {
          String test = baseDir.substring(lastFileSep);
          if (test.contains("*")) {
            baseDir = baseDir.substring(0, lastFileSep);
          }
        }
        LOG.info("base path checking " + baseDir);
        populateJarList(new File(baseDir), jarPaths, fileNames);
      }
      if (jarPaths.size() >= fileNames.size()) {
        break; // found a resource for each name in the input set, all done
      }
    }
    return jarPaths;
  }

  /**
   * Start in the working directory and recursively locate all jars.
   * @param dir current directory to explore.
   * @param fileSet the list to populate.
   * @param fileNames file names to locate.
   */
  private static void populateJarList(final File dir,
    final Set<Path> fileSet, final Set<String> fileNames) {
    File[] filesInThisDir = dir.listFiles();
    if (null == filesInThisDir) {
      return;
    }
    for (File f : dir.listFiles()) {
      if (f.isDirectory()) {
        populateJarList(f, fileSet, fileNames);
      } else if (f.isFile() && fileNames.contains(f.getName())) {
        fileSet.add(new Path(f.getAbsolutePath()));
      }
    }
  }

  /**
   * Boilerplate to add a file to the local resources..
   * @param localResources the LocalResources map to populate.
   * @param fs handle to the HDFS file system.
   * @param target the file to send to the remote container.
   */
  public static void addFileToResourceMap(Map<String, LocalResource>
    localResources, FileSystem fs, Path target)
    throws IOException {
    LocalResource resource = Records.newRecord(LocalResource.class);
    FileStatus destStatus = fs.getFileStatus(target);
    resource.setResource(ConverterUtils.getYarnUrlFromURI(target.toUri()));
    resource.setSize(destStatus.getLen());
    resource.setTimestamp(destStatus.getModificationTime());
    resource.setType(LocalResourceType.FILE); // use FILE, even for jars!
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResources.put(target.getName(), resource);
    LOG.info("Registered file in LocalResources :: " + target);
  }

  /**
   * Get the base HDFS dir we will be storing our LocalResources in.
   * @param fs the file system.
   * @param appId the ApplicationId under which our resources will be stored.
   * @return the path
   */
  public static Path getFsCachePath(final FileSystem fs,
    final ApplicationId appId) {
    return new Path(fs.getHomeDirectory(), HDFS_RESOURCE_DIR + "/" + appId);
  }

  /**
   * Popuate the environment string map to be added to the environment vars
   * in a remote execution container. Adds the local classpath to pick up
   * "yarn-site.xml" and "mapred-site.xml" stuff.
   * @param env the map of env var values.
   * @param giraphConf the GiraphConfiguration to pull values from.
   */
  public static void addLocalClasspathToEnv(final Map<String, String> env,
    final GiraphConfiguration giraphConf) {
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String cpEntry : giraphConf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':').append(cpEntry.trim()); //TODO: Separator
    }
    for (String cpEntry : giraphConf.getStrings(
      MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
      StringUtils.getStrings(
        MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
      classPathEnv.append(':').append(cpEntry.trim());
    }
    // add the runtime classpath needed for tests to work
    if (giraphConf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':').append(System.getenv("CLASSPATH"));
    }
    env.put("CLASSPATH", classPathEnv.toString());
  }

  /**
   * Populate the LocalResources list with the GiraphConf XML file's HDFS path.
   * @param giraphConf the GiraphConfifuration to export for worker tasks.
   * @param appId the ApplicationId for this YARN app.
   * @param localResourceMap the LocalResource map of files to export to tasks.
   */
  public static void addGiraphConfToLocalResourceMap(GiraphConfiguration
    giraphConf, ApplicationId appId, Map<String, LocalResource>
    localResourceMap) throws IOException {
    FileSystem fs = FileSystem.get(giraphConf);
    Path hdfsConfPath = new Path(YarnUtils.getFsCachePath(fs, appId),
      GiraphConstants.GIRAPH_YARN_CONF_FILE);
    YarnUtils.addFileToResourceMap(localResourceMap, fs, hdfsConfPath);
  }

  /**
   * Export our populated GiraphConfiguration as an XML file to be used by the
   * ApplicationMaster's exec container, and register it with LocalResources.
   * @param giraphConf the current Configuration object to be published.
   * @param appId the ApplicationId to stamp this app's base HDFS resources dir.
   */
  public static void exportGiraphConfiguration(GiraphConfiguration giraphConf,
    ApplicationId appId) throws IOException {
    File confFile = new File(System.getProperty("java.io.tmpdir"),
      GiraphConstants.GIRAPH_YARN_CONF_FILE);
    if (confFile.exists()) {
      if (!confFile.delete()) {
        LOG.warn("Unable to delete file " + confFile);
      }
    }
    String localConfPath = confFile.getAbsolutePath();
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(localConfPath);
      giraphConf.writeXml(fos);
      FileSystem fs = FileSystem.get(giraphConf);
      Path hdfsConfPath = new Path(YarnUtils.getFsCachePath(fs, appId),
        GiraphConstants.GIRAPH_YARN_CONF_FILE);
      fos.flush();
      fs.copyFromLocalFile(false, true, new Path(localConfPath), hdfsConfPath);
    } finally {
      if (null != fos) {
        fos.close();
      }
    }
  }
}
