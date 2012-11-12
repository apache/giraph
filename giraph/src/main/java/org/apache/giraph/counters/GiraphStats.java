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

package org.apache.giraph.counters;

import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Hadoop Counters in group "Giraph Stats". General statistics about job.
 */
public class GiraphStats extends HadoopCountersBase {
  /** Counter group name for the Giraph statistics */
  public static final String GROUP_NAME = "Giraph Stats";

  /** Singleton instance for everyone to use */
  private static GiraphStats INSTANCE;

  /** Superstep counter */
  private static final int SUPERSTEP = 0;
  /** Vertex counter */
  private static final int VERTICES = 1;
  /** Finished vertex counter */
  private static final int FINISHED_VERTICES = 2;
  /** Edge counter */
  private static final int EDGES = 3;
  /** Sent messages counter */
  private static final int SENT_MESSAGES = 4;
  /** Workers on this superstep */
  private static final int CURRENT_WORKERS = 5;
  /** Current master task partition */
  private static final int CURRENT_MASTER_TASK_PARTITION = 6;
  /** Last checkpointed superstep */
  private static final int LAST_CHECKPOINTED_SUPERSTEP = 7;
  /** Number of counters in this class */
  private static final int NUM_COUNTERS = 8;

  /** All the counters stored */
  private final GiraphHadoopCounter[] counters;

  /**
   * Create with Hadoop Context.
   *
   * @param context Hadoop Context to use.
   */
  private GiraphStats(Context context) {
    super(context, GROUP_NAME);
    counters = new GiraphHadoopCounter[NUM_COUNTERS];
    counters[SUPERSTEP] = getCounter("Superstep");
    counters[VERTICES] = getCounter("Aggregate vertices");
    counters[FINISHED_VERTICES] = getCounter("Aggregate finished vertices");
    counters[EDGES] = getCounter("Aggregate edges");
    counters[SENT_MESSAGES] = getCounter("Sent messages");
    counters[CURRENT_WORKERS] = getCounter("Current workers");
    counters[CURRENT_MASTER_TASK_PARTITION] =
        getCounter("Current master task partition");
    counters[LAST_CHECKPOINTED_SUPERSTEP] =
        getCounter("Last checkpointed superstep");
  }

  /**
   * Initialize with Hadoop Context.
   *
   * @param context Hadoop Context to use.
   */
  public static void init(Context context) {
    INSTANCE = new GiraphStats(context);
  }

  /**
   * Get singleton instance.
   *
   * @return GiraphStats singleton
   */
  public static GiraphStats getInstance() {
    return INSTANCE;
  }

  /**
   * Get SuperstepCounter counter
   *
   * @return SuperstepCounter counter
   */
  public GiraphHadoopCounter getSuperstepCounter() {
    return counters[SUPERSTEP];
  }

  /**
   * Get Vertices counter
   *
   * @return Vertices counter
   */
  public GiraphHadoopCounter getVertices() {
    return counters[VERTICES];
  }

  /**
   * Get FinishedVertexes counter
   *
   * @return FinishedVertexes counter
   */
  public GiraphHadoopCounter getFinishedVertexes() {
    return counters[FINISHED_VERTICES];
  }

  /**
   * Get Edges counter
   *
   * @return Edges counter
   */
  public GiraphHadoopCounter getEdges() {
    return counters[EDGES];
  }

  /**
   * Get SentMessages counter
   *
   * @return SentMessages counter
   */
  public GiraphHadoopCounter getSentMessages() {
    return counters[SENT_MESSAGES];
  }

  /**
   * Get CurrentWorkers counter
   *
   * @return CurrentWorkers counter
   */
  public GiraphHadoopCounter getCurrentWorkers() {
    return counters[CURRENT_WORKERS];
  }

  /**
   * Get CurrentMasterTaskPartition counter
   *
   * @return CurrentMasterTaskPartition counter
   */
  public GiraphHadoopCounter getCurrentMasterTaskPartition() {
    return counters[CURRENT_MASTER_TASK_PARTITION];
  }

  /**
   * Get LastCheckpointedSuperstep counter
   *
   * @return LastCheckpointedSuperstep counter
   */
  public GiraphHadoopCounter getLastCheckpointedSuperstep() {
    return counters[LAST_CHECKPOINTED_SUPERSTEP];
  }

  @Override
  public Iterator<GiraphHadoopCounter> iterator() {
    return Arrays.asList(counters).iterator();
  }
}
