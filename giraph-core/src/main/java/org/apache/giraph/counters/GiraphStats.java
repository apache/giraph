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
  /** superstep counter name */
  public static final String SUPERSTEP_NAME = "Superstep";
  /** aggregate vertices counter name */
  public static final String VERTICES_NAME = "Aggregate vertices";
  /** aggregate finished vertices counter name */
  public static final String FINISHED_VERTICES_NAME =
      "Aggregate finished vertices";
  /** aggregate edges counter name */
  public static final String EDGES_NAME = "Aggregate edges";
  /** sent messages counter name */
  public static final String SENT_MESSAGES_NAME = "Sent messages";
  /** sent message bytes counter name */
  public static final String SENT_MESSAGE_BYTES_NAME = "Sent message bytes";
  /** aggregate sent messages counter name */
  public static final String AGGREGATE_SENT_MESSAGES_NAME
    = "Aggregate sent messages";
  /** aggregate sent messages bytes counter name */
  public static final String AGGREGATE_SENT_MESSAGE_BYTES_NAME
    = "Aggregate sent message bytes";
  /** workers counter name */
  public static final String CURRENT_WORKERS_NAME = "Current workers";
  /** current master partition task counter name */
  public static final String CURRENT_MASTER_PARTITION_TASK_NAME =
      "Current master task partition";
  /** last checkpointed superstep counter name */
  public static final String LAST_CHECKPOINTED_SUPERSTEP_NAME =
      "Last checkpointed superstep";
  /** aggregate bytes loaded from local disks in out-of-core */
  public static final String OOC_BYTES_LOADED_NAME =
      "Aggregate bytes loaded from local disks (out-of-core)";
  /** aggregate bytes stored to local disks in out-of-core */
  public static final String OOC_BYTES_STORED_NAME =
      "Aggregate bytes stored to local disks (out-of-core)";
  /** lowest percentage of graph in memory throughout the execution */
  public static final String LOWEST_GRAPH_PERCENTAGE_IN_MEMORY_NAME =
      "Lowest percentage of graph in memory so far (out-of-core)";

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
  /** Sent message bytes counter */
  private static final int SENT_MESSAGE_BYTES = 8;
  /** Aggregate sent messages counter */
  private static final int AGG_SENT_MESSAGES = 9;
  /** Aggregate sent message bytes counter */
  private static final int AGG_SENT_MESSAGE_BYTES = 10;
  /** Aggregate OOC loaded bytes counter */
  private static final int OOC_BYTES_LOADED = 11;
  /** Aggregate OOC stored bytes counter */
  private static final int OOC_BYTES_STORED = 12;
  /** Lowest percentage of graph in memory over time */
  private static final int LOWEST_GRAPH_PERCENTAGE_IN_MEMORY = 13;
  /** Number of counters in this class */
  private static final int NUM_COUNTERS = 14;

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
    counters[SUPERSTEP] = getCounter(SUPERSTEP_NAME);
    counters[VERTICES] = getCounter(VERTICES_NAME);
    counters[FINISHED_VERTICES] = getCounter(FINISHED_VERTICES_NAME);
    counters[EDGES] = getCounter(EDGES_NAME);
    counters[SENT_MESSAGES] = getCounter(SENT_MESSAGES_NAME);
    counters[SENT_MESSAGE_BYTES] = getCounter(SENT_MESSAGE_BYTES_NAME);
    counters[CURRENT_WORKERS] = getCounter(CURRENT_WORKERS_NAME);
    counters[CURRENT_MASTER_TASK_PARTITION] =
        getCounter(CURRENT_MASTER_PARTITION_TASK_NAME);
    counters[LAST_CHECKPOINTED_SUPERSTEP] =
        getCounter(LAST_CHECKPOINTED_SUPERSTEP_NAME);
    counters[AGG_SENT_MESSAGES] =
        getCounter(AGGREGATE_SENT_MESSAGES_NAME);
    counters[AGG_SENT_MESSAGE_BYTES] =
        getCounter(AGGREGATE_SENT_MESSAGE_BYTES_NAME);
    counters[OOC_BYTES_LOADED] = getCounter(OOC_BYTES_LOADED_NAME);
    counters[OOC_BYTES_STORED] = getCounter(OOC_BYTES_STORED_NAME);
    counters[LOWEST_GRAPH_PERCENTAGE_IN_MEMORY] =
        getCounter(LOWEST_GRAPH_PERCENTAGE_IN_MEMORY_NAME);
    counters[LOWEST_GRAPH_PERCENTAGE_IN_MEMORY].setValue(100);
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
   * Get SentMessageBytes counter
   *
   * @return SentMessageBytes counter
   */
  public GiraphHadoopCounter getSentMessageBytes() {
    return counters[SENT_MESSAGE_BYTES];
  }

  /**
   * Get AggregateSentMessages counter
   *
   * @return AggregateSentMessages counter
   */
  public GiraphHadoopCounter getAggregateSentMessages() {
    return counters[AGG_SENT_MESSAGES];
  }

  /**
   * Get AggregateSentMessageBytes counter
   *
   * @return AggregateSentMessageBytes counter
   */
  public GiraphHadoopCounter getAggregateSentMessageBytes() {
    return counters[AGG_SENT_MESSAGE_BYTES];
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

  /**
   * Get OOCBytesLoaded counter
   *
   * @return OOCBytesLoaded counter
   */
  public GiraphHadoopCounter getAggregateOOCBytesLoaded() {
    return counters[OOC_BYTES_LOADED];
  }

  /**
   * Get OOCBytesStored counter
   *
   * @return OOCBytesStored counter
   */
  public GiraphHadoopCounter getAggregateOOCBytesStored() {
    return counters[OOC_BYTES_STORED];
  }

  public GiraphHadoopCounter getLowestGraphPercentageInMemory() {
    return counters[LOWEST_GRAPH_PERCENTAGE_IN_MEMORY];
  }

  @Override
  public Iterator<GiraphHadoopCounter> iterator() {
    return Arrays.asList(counters).iterator();
  }
}
