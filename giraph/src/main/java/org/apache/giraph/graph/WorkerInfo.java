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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Information about a worker that is sent to the master and other workers.
 */
public class WorkerInfo extends TaskInfo {
  /** Task Partition (Worker) ID of this task */
  private int taskId;
  /** Hostname + "_" + id for easier debugging */
  private String hostnameId;

  /**
   * Constructor for reflection
   */
  public WorkerInfo() {
  }

  /**
   * Constructor with parameters.
   *
   * @param taskId the task partition for this worker
   */
  public WorkerInfo(int taskId) {
    this.taskId = taskId;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public void setInetSocketAddress(InetSocketAddress address) {
    super.setInetSocketAddress(address);
    hostnameId = getHostname() + "_" + getTaskId();
  }

  public String getHostnameId() {
    return hostnameId;
  }

  @Override
  public String toString() {
    return "Worker(hostname=" + getHostname() + ", MRtaskID=" +
        getTaskId() + ", port=" + getPort() + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    taskId = input.readInt();
    hostnameId = getHostname() + "_" + getTaskId();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeInt(taskId);
  }
}
