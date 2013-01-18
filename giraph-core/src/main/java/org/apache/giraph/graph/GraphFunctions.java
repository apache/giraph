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

package org.apache.giraph.graph;

/**
 * Each compute node running on the underlying cluster
 * is marked with this enum to indicate the worker or
 * master task(s) it must perform during job runs.
 */
public enum GraphFunctions {
  /** Undecided yet */
  UNKNOWN {
    @Override public boolean isMaster() { return false; }
    @Override public boolean isWorker() { return false; }
    @Override public boolean isZooKeeper() { return false; }
  },
  /** Only be the master */
  MASTER_ONLY {
    @Override public boolean isMaster() { return true; }
    @Override public boolean isWorker() { return false; }
    @Override public boolean isZooKeeper() { return false; }
  },
  /** Only be the master and ZooKeeper */
  MASTER_ZOOKEEPER_ONLY {
    @Override public boolean isMaster() { return true; }
    @Override public boolean isWorker() { return false; }
    @Override public boolean isZooKeeper() { return true; }
  },
  /** Only be the worker */
  WORKER_ONLY {
    @Override public boolean isMaster() { return false; }
    @Override public boolean isWorker() { return true; }
    @Override public boolean isZooKeeper() { return false; }
  },
  /** Do master, worker, and ZooKeeper */
  ALL {
    @Override public boolean isMaster() { return true; }
    @Override public boolean isWorker() { return true; }
    @Override public boolean isZooKeeper() { return true; }
  },
  /** Do master and worker */
  ALL_EXCEPT_ZOOKEEPER {
    @Override public boolean isMaster() { return true; }
    @Override public boolean isWorker() { return true; }
    @Override public boolean isZooKeeper() { return false; }
  };

  /**
   * Tell whether this function acts as a master.
   *
   * @return true iff this map function is a master
   */
  public abstract boolean isMaster();

  /**
   * Tell whether this function acts as a worker.
   *
   * @return true iff this map function is a worker
   */
  public abstract boolean isWorker();

  /**
   * Tell whether this function acts as a ZooKeeper server.
   *
   * @return true iff this map function is a zookeeper server
   */
  public abstract boolean isZooKeeper();

  public boolean isKnown() {
    return this != UNKNOWN;
  }

  public boolean isUnknown() {
    return !isKnown();
  }

  public boolean isNotAWorker() {
    return isKnown() && !isWorker();
  }
}
