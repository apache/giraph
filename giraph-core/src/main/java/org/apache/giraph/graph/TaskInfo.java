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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Abstract class for information about any task - worker or master.
 */
public abstract class TaskInfo implements Writable {
  /** Task hostname */
  private String hostname;
  /** Port that the IPC server is using */
  private int port;
  /** Task partition id */
  private int taskId = -1;
  /** Task host IP */
  private String hostOrIp;

  /**
   * Constructor
   */
  public TaskInfo() {
  }

  /**
   * Get this task's hostname
   *
   * @return Hostname
   */
  public String getHostname() {
    return hostname.toLowerCase();
  }

  /**
   * Get this task's host address. Could be IP.
   *
   * @return host address
   */
  public String getHostOrIp() {
    return hostOrIp;
  }

  /**
   * Get port that the IPC server of this task is using
   *
   * @return Port
   */
  public int getPort() {
    return port;
  }

  /**
   * Set address that the IPC server of this task is using
   *
   * @param address Address
   * @param host host name or IP
   */
  public void setInetSocketAddress(InetSocketAddress address, String host) {
    this.port = address.getPort();
    this.hostname = address.getHostName();
    this.hostOrIp = host;
  }

  /**
   * Get a new instance of the InetSocketAddress for this hostname and port
   *
   * @return InetSocketAddress of the hostname and port.
   */
  public InetSocketAddress getInetSocketAddress() {
    return new InetSocketAddress(hostOrIp, port);
  }

  /**
   * Set task partition id of this task
   *
   * @param taskId partition id
   */
  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  /**
   * Get task partition id of this task
   *
   * @return Task partition id of this task
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * Get hostname and task id
   *
   * @return Hostname and task id
   */
  public String getHostnameId() {
    return getHostname() + "_" + getTaskId();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskInfo) {
      TaskInfo taskInfo = (TaskInfo) other;
      if (getHostname().equals(taskInfo.getHostname()) &&
          getHostOrIp().equals(taskInfo.getHostOrIp()) &&
          (getTaskId() == taskInfo.getTaskId()) &&
          (port == taskInfo.getPort() &&
          (taskId == taskInfo.getTaskId()))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "hostname=" + getHostname() +
        " hostOrIp=" + getHostOrIp() +
        ", MRtaskID=" + getTaskId() +
        ", port=" + getPort();
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    hostname = input.readUTF();
    hostOrIp = input.readUTF();
    port = input.readInt();
    taskId = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(hostname);
    output.writeUTF(hostOrIp);
    output.writeInt(port);
    output.writeInt(taskId);
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + getPort();
    result = 37 * result + hostname.hashCode();
    result = 37 * result + hostOrIp.hashCode();
    result = 37 * result + getTaskId();
    return result;
  }

}
