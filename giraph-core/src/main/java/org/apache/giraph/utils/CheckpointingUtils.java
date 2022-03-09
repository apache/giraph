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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;

import static org.apache.giraph.conf.GiraphConstants.CHECKPOINT_DIRECTORY;

/**
 * Holds useful functions to get checkpoint paths
 * in hdfs.
 */
public class CheckpointingUtils {

  /** If at the end of a checkpoint file, indicates metadata */
  public static final String CHECKPOINT_METADATA_POSTFIX = ".metadata";
  /**
   * If at the end of a checkpoint file, indicates vertices, edges,
   * messages, etc.
   */
  public static final String CHECKPOINT_VERTICES_POSTFIX = ".vertices";
  /**
   * If at the end of a checkpoint file, indicates metadata and data is valid
   * for the same filenames without .valid
   */
  public static final String CHECKPOINT_VALID_POSTFIX = ".valid";
  /**
   * If at the end of a checkpoint file,
   * indicates that we store WorkerContext and aggregator handler data.
   */
  public static final String CHECKPOINT_DATA_POSTFIX = ".data";
  /**
   * If at the end of a checkpoint file, indicates the stitched checkpoint
   * file prefixes.  A checkpoint is not valid if this file does not exist.
   */
  public static final String CHECKPOINT_FINALIZED_POSTFIX = ".finalized";

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CheckpointingUtils.class);

  /**
   * Do not call constructor.
   */
  private CheckpointingUtils() {
  }

  /**
   * Path to the checkpoint's root (including job id)
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return checkpoint's root
   */
  public static String getCheckpointBasePath(Configuration conf,
                                             String jobId) {
    return CHECKPOINT_DIRECTORY.getWithDefault(conf,
        CHECKPOINT_DIRECTORY.getDefaultValue() + "/" + jobId);
  }

  /**
   * Path to checkpoint&amp;halt node in hdfs.
   * It is set to let client know that master has
   * successfully finished checkpointing and job can be restarted.
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return path to checkpoint&amp;halt node in hdfs.
   */
  public static Path getCheckpointMarkPath(Configuration conf,
                                           String jobId) {
    return new Path(getCheckpointBasePath(conf, jobId), "halt");
  }

  /**
   * Get the last saved superstep.
   *
   * @param fs file system where checkpoint is stored.
   * @param checkpointBasePath path to checkpoints folder
   * @return Last good superstep number
   * @throws java.io.IOException
   */
  public static long getLastCheckpointedSuperstep(
      FileSystem fs, String checkpointBasePath) throws IOException {
    Path cpPath = new Path(checkpointBasePath);
    if (fs.exists(cpPath)) {
      FileStatus[] fileStatusArray =
          fs.listStatus(cpPath, new FinalizedCheckpointPathFilter());
      if (fileStatusArray != null) {
        long lastCheckpointedSuperstep = Long.MIN_VALUE;
        for (FileStatus file : fileStatusArray) {
          long superstep = getCheckpoint(file);
          if (superstep > lastCheckpointedSuperstep) {
            lastCheckpointedSuperstep = superstep;
          }
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("getLastGoodCheckpoint: Found last good checkpoint " +
              lastCheckpointedSuperstep);
        }
        return lastCheckpointedSuperstep;
      }
    }
    return -1;
  }

  /**
   * Get the checkpoint from a finalized checkpoint path
   *
   * @param finalizedPath Path of the finalized checkpoint
   * @return Superstep referring to a checkpoint of the finalized path
   */
  private static long getCheckpoint(FileStatus finalizedPath) {
    if (!finalizedPath.getPath().getName().
        endsWith(CHECKPOINT_FINALIZED_POSTFIX)) {
      throw new InvalidParameterException(
          "getCheckpoint: " + finalizedPath + "Doesn't end in " +
              CHECKPOINT_FINALIZED_POSTFIX);
    }
    String checkpointString =
        finalizedPath.getPath().getName().
            replace(CHECKPOINT_FINALIZED_POSTFIX, "");
    return Long.parseLong(checkpointString);
  }


  /**
   * Only get the finalized checkpoint files
   */
  private static class FinalizedCheckpointPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(CHECKPOINT_FINALIZED_POSTFIX);
    }

  }
}
