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

package org.apache.giraph.ooc.persistence;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.ONE_MB;

/**
 * Data accessor object to read/write data in local disk.
 * Note: This class assumes that the data are partitioned across IO threads,
 *       i.e. each part of data can be accessed by one and only one IO thread
 *       throughout the execution. Also, each IO thread reads a particular
 *       type of data completely and, only then, it can read other type of data;
 *       i.e. an IO thread cannot be used to read two different files at the
 *       same time. These assumptions are based on the assumptions that the
 *       current out-of-core mechanism is designed for.
 */
public class LocalDiskDataAccessor implements OutOfCoreDataAccessor {
  /**
   * Size of the buffer used for (de)serializing data when reading/writing
   * from/to disk
   */
  public static final IntConfOption OOC_DISK_BUFFER_SIZE =
      new IntConfOption("graph.oocDiskBufferSize", 4 * ONE_MB,
          "size of the buffer when (de)serializing data for reading/writing " +
              "from/to disk");

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LocalDiskDataAccessor.class);
  /**
   * In-memory buffer used for (de)serializing data when reading/writing
   * from/to disk using Kryo
   */
  private final byte[][] perThreadBuffers;
  /** Path prefix for different disks */
  private final String[] basePaths;
  /** How many disks (i.e. IO threads) do we have? */
  private final int numDisks;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public LocalDiskDataAccessor(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf) {
    // Take advantage of multiple disks
    String[] userPaths = GiraphConstants.PARTITIONS_DIRECTORY.getArray(conf);
    this.numDisks = userPaths.length;
    if (!GiraphConstants.NUM_OUT_OF_CORE_THREADS.isDefaultValue(conf) ||
        GiraphConstants.NUM_OUT_OF_CORE_THREADS.get(conf) != numDisks) {
      LOG.warn("LocalDiskDataAccessor: with this data accessor, number of " +
          "out-of-core threads is only specified by the number of " +
          "directories given by 'giraph.partitionsDirectory' flag! Now using " +
          numDisks + " IO threads!");
    }
    this.basePaths = new String[numDisks];
    int ptr = 0;
    String jobId = conf.getJobId();
    for (String path : userPaths) {
      String jobDirectory = path + "/" + jobId;
      File file = new File(jobDirectory);
      checkState(file.mkdirs(), "LocalDiskDataAccessor: cannot create " +
          "directory " + file.getAbsolutePath());
      basePaths[ptr] = jobDirectory + "/";
      ptr++;
    }
    final int diskBufferSize = OOC_DISK_BUFFER_SIZE.get(conf);
    this.perThreadBuffers = new byte[numDisks][diskBufferSize];
  }

  @Override
  public void initialize() { }

  @Override
  public void shutdown() {
    for (String path : basePaths) {
      File file = new File(path);
      for (String subFileName : file.list()) {
        File subFile = new File(file.getPath(), subFileName);
        checkState(subFile.delete(), "shutdown: cannot delete file %s",
            subFile.getAbsoluteFile());
      }
      checkState(file.delete(), "shutdown: cannot delete directory %s",
          file.getAbsoluteFile());
    }
  }

  @Override
  public int getNumAccessorThreads() {
    return numDisks;
  }

  @Override
  public DataInputWrapper prepareInput(int threadId, DataIndex index)
      throws IOException {
    return new LocalDiskDataInputWrapper(basePaths[threadId] + index.toString(),
        perThreadBuffers[threadId]);
  }

  @Override
  public DataOutputWrapper prepareOutput(
      int threadId, DataIndex index, boolean shouldAppend) throws IOException {
    return new LocalDiskDataOutputWrapper(
        basePaths[threadId] + index.toString(), shouldAppend,
        perThreadBuffers[threadId]);
  }

  @Override
  public boolean dataExist(int threadId, DataIndex index) {
    return new File(basePaths[threadId] + index.toString()).exists();
  }

  /** Implementation of <code>DataInput</code> wrapper for local disk reader */
  private static class LocalDiskDataInputWrapper implements DataInputWrapper {
    /** File used to read the data from */
    private final File file;
    /** Kryo's handle to read the data */
    private final Input input;

    /**
     * Constructor
     *
     * @param fileName file name
     * @param buffer reusable byte buffer that will be used in Kryo's Input
     *               reader
     * @throws IOException
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        "OBL_UNSATISFIED_OBLIGATION")
    LocalDiskDataInputWrapper(String fileName, byte[] buffer)
        throws IOException {
      file = new File(fileName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("LocalDiskDataInputWrapper: obtaining a data input from " +
            "local file " + file.getAbsolutePath());
      }
      input = new UnsafeInput(buffer);
      input.setInputStream(new FileInputStream(
          new RandomAccessFile(file, "r").getFD()));
    }

    @Override
    public DataInput getDataInput() {
      return new KryoDataInput(input);
    }

    @Override
    public long finalizeInput(boolean deleteOnClose) {
      input.close();
      long count = input.total();
      checkState(!deleteOnClose || file.delete(),
          "finalizeInput: failed to delete %s.", file.getAbsoluteFile());
      return count;
    }
  }

  /** Implementation of <code>DataOutput</code> wrapper for local disk writer */
  private static class LocalDiskDataOutputWrapper implements DataOutputWrapper {
    /** File used to write the data to */
    private final File file;
    /** Kryo's handle to write the date */
    private final Output output;

    /**
     * Constructor
     *
     * @param fileName file name
     * @param shouldAppend whether the <code>DataOutput</code> should be used
     *                     for appending to already existing files
     * @param buffer reusable byte buffer that will be used in Kryo's Output
     *               writer
     * @throws IOException
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        "OBL_UNSATISFIED_OBLIGATION")
    LocalDiskDataOutputWrapper(String fileName, boolean shouldAppend,
                               byte[] buffer) throws IOException {
      file = new File(fileName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("LocalDiskDataOutputWrapper: obtaining a data output from " +
            "local file " + file.getAbsolutePath());
        if (!shouldAppend) {
          checkState(!file.exists(), "LocalDiskDataOutputWrapper: file %s " +
              "already exist", file.getAbsoluteFile());
          checkState(file.createNewFile(), "LocalDiskDataOutputWrapper: " +
              "cannot create file %s", file.getAbsolutePath());
        }
      }
      output = new UnsafeOutput(buffer);
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      if (shouldAppend) {
        raf.seek(file.length());
      }
      output.setOutputStream(new FileOutputStream(raf.getFD()));
    }

    @Override
    public DataOutput getDataOutput() {
      return new KryoDataOutput(output);
    }


    @Override
    public long finalizeOutput() {
      output.close();
      long count = output.total();
      return count;
    }
  }
}
