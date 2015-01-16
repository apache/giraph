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

package org.apache.giraph.metrics;

/**
 * Class holding metric names used in Giraph
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface MetricNames {
// CHECKSTYLE: resume InterfaceIsTypeCheck

  //////////////////////////////////////////////////////////////////////////////
  // Request counters per superstep
  //////////////////////////////////////////////////////////////////////////////
  /** Counter of requests handled locally */
  String LOCAL_REQUESTS = "local-requests";
  /** Counter of requets sent on the wire */
  String REMOTE_REQUESTS = "remote-requests";
  /** Guage of total requests */
  String TOTAL_REQUESTS = "total-requests";
  /** PercentGauge of requests that are handled locally */
  String PERCENT_LOCAL_REQUESTS = "percent-local-requests";

  /** Counter for sending vertices requests */
  String SEND_VERTEX_REQUESTS = "send-vertex-requests";
  /** Counter for sending a partition of messages for next superstep */
  String SEND_WORKER_MESSAGES_REQUESTS = "send-worker-messages-requests";
  /**
   * Counter for sending a partition of messages for current superstep
   * (used during partition exchange)
   */
  String SEND_PARTITION_CURRENT_MESSAGES_REQUESTS =
      "send-partition-current-messages-requests";
  /** Counter for sending a partition of mutations */
  String SEND_PARTITION_MUTATIONS_REQUESTS =
      "send-partition-mutations-requests";
  /** Counter for sending aggregated values from one worker's vertices */
  String SEND_WORKER_AGGREGATORS_REQUESTS = "send-worker-aggregators-requests";
  /** Counter for sending aggregated values from worker owner to master */
  String SEND_AGGREGATORS_TO_MASTER_REQUESTS =
      "send-aggregators-to-master-requests";
  /** Counter for sending aggregators from master to worker owners */
  String SEND_AGGREGATORS_TO_OWNER_REQUESTS =
      "send-aggregators-to-owner-requests";
  /** Counter for sending aggregators from worker owner to other workers */
  String SEND_AGGREGATORS_TO_WORKER_REQUESTS =
      "send-aggregators-to-worker-requests";

  /** Counter for time spent waiting on too many open requests */
  String TIME_SPENT_WAITING_ON_TOO_MANY_OPEN_REQUESTS_MS =
      "time-spent-waiting-on-too-many-open-requests-ms";
  //////////////////////////////////////////////////////////////////////////////
  // End of Request counters per superstep
  //////////////////////////////////////////////////////////////////////////////

  /** Counter of messages sent in superstep */
  String MESSAGES_SENT = "messages-sent";

  /** Counter of messages sent in superstep */
  String MESSAGE_BYTES_SENT = "message-bytes-sent";

  /** Histogram for vertices in mutations requests */
  String VERTICES_IN_MUTATION_REQUEST = "vertices-per-mutations-request";

  /** Number of bytes sent in superstep */
  String SENT_BYTES = "sent-bytes";
  /** Number of bytes received in superstep */
  String RECEIVED_BYTES = "received-bytes";

  /** PercentGauge of memory free */
  String MEMORY_FREE_PERCENT = "memory-free-pct";

  /** Total edges loaded */
  String EDGES_FILTERED = "edges-filtered";
  /** Percent of edges filtered out */
  String EDGES_FILTERED_PCT = "edges-filtered-pct";

  /** Total vertices filtered */
  String VERTICES_FILTERED = "vertices-filtered";
  /** Percent of vertices filtered out */
  String VERTICES_FILTERED_PCT = "vertices-filtered-pct";

  /** Name of metric for compute times per partition */
  String HISTOGRAM_COMPUTE_PER_PARTITION = "compute-per-partition-ms";
}
