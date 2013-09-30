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

package org.apache.giraph.io.formats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]
import org.apache.hadoop.mapreduce.security.TokenCache;
end[HADOOP_NON_SECURE]*/

/**
 * Provides functionality similar to {@link FileInputFormat},
 * but allows for different data sources (vertex and edge data).
 *
 * @param <K> Key
 * @param <V> Value
 */
public abstract class GiraphFileInputFormat<K, V>
    extends FileInputFormat<K, V> {
  /** Vertex input file paths. */
  public static final String VERTEX_INPUT_DIR = "giraph.vertex.input.dir";
  /** Edge input file paths. */
  public static final String EDGE_INPUT_DIR = "giraph.edge.input.dir";
  /** Number of vertex input files. */
  public static final String NUM_VERTEX_INPUT_FILES =
      "giraph.input.vertex.num.files";
  /** Number of edge input files. */
  public static final String NUM_EDGE_INPUT_FILES =
      "giraph.input.edge.num.files";

  /** Split slop. */
  private static final double SPLIT_SLOP = 1.1; // 10% slop

  /** Filter for hidden files. */
  private static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(GiraphFileInputFormat.class);

  /**
   * Add a {@link org.apache.hadoop.fs.Path} to the list of vertex inputs.
   *
   * @param conf the Configuration to store the input paths
   * @param path {@link org.apache.hadoop.fs.Path} to be added to the list of
   *                                              vertex inputs
   */
  public static void addVertexInputPath(Configuration conf,
    Path path) throws IOException {
    String dirStr = pathToDirString(conf, path);
    String dirs = conf.get(VERTEX_INPUT_DIR);
    conf.set(VERTEX_INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
  }

  /**
   * Set the {@link Path} for vertex input.
   * @param conf Configuration to store in
   * @param path {@link Path} to set
   * @throws IOException on I/O errors
   */
  public static void setVertexInputPath(Configuration conf,
      Path path) throws IOException {
    conf.set(VERTEX_INPUT_DIR, pathToDirString(conf, path));
  }

  /**
   * Add a {@link org.apache.hadoop.fs.Path} to the list of edge inputs.
   *
   * @param conf the Configuration to store the input paths
   * @param path {@link org.apache.hadoop.fs.Path} to be added to the list of
   *                                              edge inputs
   */
  public static void addEdgeInputPath(Configuration conf,
    Path path) throws IOException {
    String dirStr = pathToDirString(conf, path);
    String dirs = conf.get(EDGE_INPUT_DIR);
    conf.set(EDGE_INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
  }

  /**
   * Set the {@link Path} for edge input.
   * @param conf Configuration to store in
   * @param path {@link Path} to set
   * @throws IOException on I/O errors
   */
  public static void setEdgeInputPath(Configuration conf,
      Path path) throws IOException {
    conf.set(EDGE_INPUT_DIR, pathToDirString(conf, path));
  }

  /**
   * Convert from a Path to a string.
   * This makes the path fully qualified and does escaping.
   *
   * @param conf Configuration to use
   * @param path Path to convert
   * @return String of escaped dir
   * @throws IOException on I/O errors
   */
  private static String pathToDirString(Configuration conf, Path path)
    throws IOException {
    path = path.getFileSystem(conf).makeQualified(path);
    return StringUtils.escapeString(path.toString());
  }

  /**
   * Get the list of vertex input {@link Path}s.
   *
   * @param context The job
   * @return The list of input {@link Path}s
   */
  public static Path[] getVertexInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(VERTEX_INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * Get the list of edge input {@link Path}s.
   *
   * @param context The job
   * @return The list of input {@link Path}s
   */
  public static Path[] getEdgeInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(EDGE_INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * HIDDEN_FILE_FILTER together with a user provided one (if any).
   */
  private static class MultiPathFilter implements PathFilter {
    /** List of filters. */
    private List<PathFilter> filters;

    /**
     * Constructor.
     *
     * @param filters The list of filters
     */
    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    /**
     * True iff all filters accept the given path.
     *
     * @param path The path to check
     * @return Whether the path is accepted
     */
    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Common method for listing vertex/edge input directories.
   *
   * @param job The job
   * @param dirs list of vertex/edge input paths
   * @return Array of FileStatus objects
   * @throws IOException
   */
  private List<FileStatus> listStatus(JobContext job, Path[] dirs)
    throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]
    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
        job.getConfiguration());
end[HADOOP_NON_SECURE]*/

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the HIDDEN_FILE_FILTER and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(HIDDEN_FILE_FILTER);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (Path p : dirs) {
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDir()) {
            Collections.addAll(result, fs.listStatus(globStat.getPath(),
                inputFilter));
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  /**
   * List vertex input directories.
   *
   * @param job the job to list vertex input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected List<FileStatus> listVertexStatus(JobContext job)
    throws IOException {
    return listStatus(job, getVertexInputPaths(job));
  }

  /**
   * List edge input directories.
   *
   * @param job the job to list edge input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected List<FileStatus> listEdgeStatus(JobContext job)
    throws IOException {
    return listStatus(job, getEdgeInputPaths(job));
  }

  /**
   * Common method for generating the list of vertex/edge input splits.
   *
   * @param job The job
   * @param files Array of FileStatus objects for vertex/edge input files
   * @return The list of vertex/edge input splits
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, List<FileStatus> files)
    throws IOException {
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();

    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job.getConfiguration());
      long length = file.getLen();
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      if ((length != 0) && isSplitable(job, path)) {
        long blockSize = file.getBlockSize();
        long splitSize = computeSplitSize(blockSize, minSize, maxSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
          int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
          splits.add(new FileSplit(path, length - bytesRemaining, splitSize,
              blkLocations[blkIndex].getHosts()));
          bytesRemaining -= splitSize;
        }

        if (bytesRemaining != 0) {
          splits.add(new FileSplit(path, length - bytesRemaining,
              bytesRemaining,
              blkLocations[blkLocations.length - 1].getHosts()));
        }
      } else if (length != 0) {
        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
      } else {
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(path, 0, length, new String[0]));
      }
    }
    return splits;
  }

  /**
   * Generate the list of vertex input splits.
   *
   * @param job The job
   * @return The list of vertex input splits
   * @throws IOException
   */
  public List<InputSplit> getVertexSplits(JobContext job) throws IOException {
    List<FileStatus> files = listVertexStatus(job);
    List<InputSplit> splits = getSplits(job, files);
    // Save the number of input files in the job-conf
    job.getConfiguration().setLong(NUM_VERTEX_INPUT_FILES, files.size());
    LOG.debug("Total # of vertex splits: " + splits.size());
    return splits;
  }

  /**
   * Generate the list of edge input splits.
   *
   * @param job The job
   * @return The list of edge input splits
   * @throws IOException
   */
  public List<InputSplit> getEdgeSplits(JobContext job) throws IOException {
    List<FileStatus> files = listEdgeStatus(job);
    List<InputSplit> splits = getSplits(job, files);
    // Save the number of input files in the job-conf
    job.getConfiguration().setLong(NUM_EDGE_INPUT_FILES, files.size());
    LOG.debug("Total # of edge splits: " + splits.size());
    return splits;
  }
}
