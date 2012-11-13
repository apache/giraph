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
    return hostname;
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
   */
  public void setInetSocketAddress(InetSocketAddress address) {
    this.port = address.getPort();
    this.hostname = address.getHostName();
  }

  /**
   * Get a new instance of the InetSocketAddress for this hostname and port
   *
   * @return InetSocketAddress of the hostname and port.
   */
  public InetSocketAddress getInetSocketAddress() {
    return new InetSocketAddress(hostname, port);
  }

  /**
   * Get task partition id of this task
   *
   * @return Task partition id of this task
   */
  public abstract int getTaskId();

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskInfo) {
      TaskInfo taskInfo = (TaskInfo) other;
      if (hostname.equals(taskInfo.getHostname()) &&
          (getTaskId() == taskInfo.getTaskId()) &&
          (port == taskInfo.getPort())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    hostname = input.readUTF();
    port = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(hostname);
    output.writeInt(port);
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + getPort();
    result = 37 * result + hostname.hashCode();
    result = 37 * result + getTaskId();
    return result;
  }
}
