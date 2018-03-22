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

package org.apache.giraph.comm.netty;

import java.net.InetSocketAddress;
import java.util.List;
import com.google.common.collect.Lists;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;


/**
 * Maintains multiple channels and rotates between them.  This is thread-safe.
 */
public class ChannelRotater {
  /** Index of last used channel */
  private int index = 0;
  /** Channel list */
  private final List<Channel> channelList = Lists.newArrayList();
  /** Task id of this channel */
  private final Integer taskId;
  /** Address these channels are associated with */
  private final InetSocketAddress address;

  /**
   * Constructor
   *
   * @param taskId Id of the task these channels as associated with
   * @param address Address these channels are associated with
   */
  public ChannelRotater(Integer taskId, InetSocketAddress address) {
    this.taskId = taskId;
    this.address = address;
  }

  public Integer getTaskId() {
    return taskId;
  }

  /**
   * Add a channel to the rotation
   *
   * @param channel Channel to add
   */
  public synchronized void addChannel(Channel channel) {
    synchronized (channelList) {
      channelList.add(channel);
    }
  }

  /**
   * Get the next channel
   *
   * @return Next channel
   */
  public synchronized Channel nextChannel() {
    if (channelList.isEmpty()) {
      throw new IllegalArgumentException(
          "nextChannel: No channels exist for hostname " +
              address.getHostName());
    }

    ++index;
    if (index >= channelList.size()) {
      index = 0;
    }
    return channelList.get(index);
  }

  /**
   * Remove the a channel
   *
   * @param channel Channel to remove
   * @return Return true if successful, false otherwise
   */
  public synchronized boolean removeChannel(Channel channel) {
    boolean success = channelList.remove(channel);
    if (index >= channelList.size()) {
      index = 0;
    }
    return success;
  }

  /**
   * Get the number of channels in this object
   *
   * @return Number of channels
   */
  public synchronized int size() {
    return channelList.size();
  }

  /**
   * Close the channels
   *
   * @param channelFutureListener If desired, pass in a channel future listener
   */
  public synchronized void closeChannels(
      ChannelFutureListener channelFutureListener) {
    for (Channel channel : channelList) {
      ChannelFuture channelFuture = channel.close();
      if (channelFutureListener != null) {
        channelFuture.addListener(channelFutureListener);
      }
    }
  }

  /**
   * Get a copy of the channels
   *
   * @return Copy of the channels
   */
  public synchronized Iterable<Channel> getChannels() {
    return Lists.newArrayList(channelList);
  }
}
