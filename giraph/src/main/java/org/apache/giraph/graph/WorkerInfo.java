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

import org.apache.hadoop.io.Writable;

/**
 * Information about a worker that is sent to the master and other workers.
 */
public class WorkerInfo implements Writable {
  /** Worker hostname */
  private String hostname;
  /** Task Partition (Worker) ID of this worker */
  private int taskId = -1;
  /** Port that the IPC server is using */
  private int port = -1;
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
   * @param hostname Hostname of this worker.
   * @param taskId the task partition for this worker
   * @param port Port of the service.
   */
  public WorkerInfo(String hostname, int taskId, int port) {
    this.hostname = hostname;
    this.taskId = taskId;
    this.port = port;
    this.hostnameId = hostname + "_" + taskId;
  }

  public String getHostname() {
    return hostname;
  }

  public int getTaskId() {
    return taskId;
  }

  public String getHostnameId() {
    return hostnameId;
  }

  /**
   * Get a new instance of the InetSocketAddress for this hostname and port
   *
   * @return InetSocketAddress of the hostname and port.
   */
  public InetSocketAddress getInetSocketAddress() {
    return new InetSocketAddress(hostname, port);
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WorkerInfo) {
      WorkerInfo workerInfo = (WorkerInfo) other;
      if (hostname.equals(workerInfo.getHostname()) &&
          (taskId == workerInfo.getTaskId()) &&
          (port == workerInfo.getPort())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + port;
    result = 37 * result + hostname.hashCode();
    result = 37 * result + taskId;
    return result;
  }

  @Override
  public String toString() {
    return "Worker(hostname=" + hostname + ", MRtaskID=" +
        taskId + ", port=" + port + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    hostname = input.readUTF();
    taskId = input.readInt();
    port = input.readInt();
    hostnameId = hostname + "_" + taskId;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(hostname);
    output.writeInt(taskId);
    output.writeInt(port);
  }
}
