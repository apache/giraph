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
package org.apache.giraph.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import java.io.IOException;

import static org.apache.commons.io.FilenameUtils.getBaseName;

/**
 * Helpers for dealing with {@link org.apache.hadoop.filecache.DistributedCache}
 */
public class DistributedCacheUtils {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(
      DistributedCacheUtils.class);

  /** Don't construct */
  private DistributedCacheUtils() { }

  /**
   * Get local path to file from a DistributedCache.
   *
   * @param conf Configuration
   * @param pathToMatch Path that was used to insert into DistributedCache
   * @return Path matched, or Optional.absent()
   */
  public static Optional<Path>
  getLocalCacheFile(Configuration conf, String pathToMatch) {
    String nameToPath = FilenameUtils.getName(pathToMatch);
    Path[] paths;
    try {
      paths = DistributedCache.getLocalCacheFiles(conf);
    } catch (IOException e) {
      return Optional.absent();
    }
    for (Path path : paths) {
      if (FilenameUtils.getName(path.toString()).equals(nameToPath)) {
        return Optional.of(path);
      }
    }
    return Optional.absent();
  }

  /**
   * Copy a file to HDFS if it is local. If the path is already in HDFS, this
   * call does nothing.
   *
   * @param path path to file
   * @param conf Configuration
   * @return path to file on HDFS.
   */
  public static Path copyToHdfs(Path path, Configuration conf) {
    if (path.toString().startsWith("hdfs://")) {
      // Already on HDFS
      return path;
    }

    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to get HDFS FileSystem", e);
    }
    String name = getBaseName(path.toString()) + "-" + System.nanoTime();
    Path remotePath = new Path("/tmp/giraph", name);
    LOG.info("copyToHdfsIfNecessary: Copying " + path + " to " +
        remotePath + " on hdfs " + fs.getUri());
    try {
      fs.copyFromLocalFile(false, true, path, remotePath);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to copy jython script from local path " + path +
          " to hdfs path " + remotePath + " on hdfs " + fs.getUri(), e);
    }
    return remotePath;
  }

  /**
   * Copy a file to HDFS if it is local, and adds it to the distributed cache.
   *
   * @param path path to file
   * @param conf Configuration
   * @return remote path to file
   */
  public static Path copyAndAdd(Path path, Configuration conf) {
    Path remotePath = copyToHdfs(path, conf);
    DistributedCache.addCacheFile(remotePath.toUri(), conf);
    return remotePath;
  }
}
