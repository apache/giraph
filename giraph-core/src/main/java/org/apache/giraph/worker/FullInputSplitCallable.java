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

package org.apache.giraph.worker;

import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * InputSplitCallable to read all the splits
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 */
public abstract class FullInputSplitCallable<I extends WritableComparable,
  V extends Writable, E extends Writable>
  implements Callable<Integer> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(
    FullInputSplitCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.get();
  /** Configuration */
  protected final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Context */
  protected final Mapper<?, ?, ?, ?>.Context context;

  /** The List of InputSplit znode paths */
  private final List<String> pathList;
  /** Current position in the path list */
  private final AtomicInteger currentIndex;
  /** ZooKeeperExt handle */
  private final ZooKeeperExt zooKeeperExt;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();

  // CHECKSTYLE: stop ParameterNumberCheck
  /**
   * Constructor.

   * @param splitOrganizer Input splits organizer
   * @param context Context
   * @param configuration Configuration
   * @param zooKeeperExt Handle to ZooKeeperExt
   * @param currentIndex Atomic Integer to get splitPath from list
   */
  public FullInputSplitCallable(InputSplitPathOrganizer splitOrganizer,
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      ZooKeeperExt zooKeeperExt,
      AtomicInteger currentIndex) {
    this.pathList = Lists.newArrayList(splitOrganizer.getPathList());
    this.currentIndex = currentIndex;
    this.zooKeeperExt = zooKeeperExt;
    this.context = context;
    this.configuration = configuration;
  }
  // CHECKSTYLE: resume ParameterNumberCheck

  /**
   * Get input format
   *
   * @return Input format
   */
  public abstract GiraphInputFormat getInputFormat();

  /**
   * Load mapping entries from all the given input splits
   *
   * @param inputSplit Input split to load
   * @return Count of vertices and edges loaded
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  protected abstract Integer readInputSplit(InputSplit inputSplit)
    throws IOException, InterruptedException;

  @Override
  public Integer call() {
    int entries = 0;
    String inputSplitPath;
    int inputSplitsProcessed = 0;
    try {
      while (true) {
        int pos = currentIndex.getAndIncrement();
        if (pos >= pathList.size()) {
          break;
        }
        inputSplitPath = pathList.get(pos);
        entries += loadInputSplit(inputSplitPath);
        context.progress();
        ++inputSplitsProcessed;
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException("call: InterruptedException", e);
    } catch (IOException e) {
      throw new IllegalStateException("call: IOException", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("call: ClassNotFoundException", e);
    } catch (InstantiationException e) {
      throw new IllegalStateException("call: InstantiationException", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("call: IllegalAccessException", e);
    }

    if (LOG.isInfoEnabled()) {
      float seconds = Times.getNanosSince(TIME, startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      float entriesPerSecond = entries / seconds;
      LOG.info("call: Loaded " + inputSplitsProcessed + " " +
          "input splits in " + seconds + " secs, " + entries +
          " " + entriesPerSecond + " entries/sec");
    }
    return entries;
  }

  /**
   * Extract entries from input split, saving them into mapping store.
   * Mark the input split finished when done.
   *
   * @param inputSplitPath ZK location of input split
   * @return Number of entries read in this input split
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private Integer loadInputSplit(
    String inputSplitPath)
    throws IOException, ClassNotFoundException, InterruptedException,
    InstantiationException, IllegalAccessException {
    InputSplit inputSplit = getInputSplit(inputSplitPath);
    Integer entriesRead = readInputSplit(inputSplit);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadFromInputSplit: Finished loading " +
          inputSplitPath + " " + entriesRead);
    }
    return entriesRead;
  }

  /**
   * Talk to ZooKeeper to convert the input split path to the actual
   * InputSplit.
   *
   * @param inputSplitPath Location in ZK of input split
   * @return instance of InputSplit
   * @throws IOException
   * @throws ClassNotFoundException
   */
  protected InputSplit getInputSplit(String inputSplitPath)
    throws IOException, ClassNotFoundException {
    byte[] splitList;
    try {
      splitList = zooKeeperExt.getData(inputSplitPath, false, null);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getInputSplit: KeeperException on " + inputSplitPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getInputSplit: IllegalStateException on " + inputSplitPath, e);
    }
    context.progress();

    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(splitList));
    InputSplit inputSplit = getInputFormat().readInputSplit(inputStream);

    if (LOG.isInfoEnabled()) {
      LOG.info("getInputSplit: Processing " + inputSplitPath +
          " from ZooKeeper and got input split '" +
          inputSplit.toString() + "'");
    }
    return inputSplit;
  }
}
