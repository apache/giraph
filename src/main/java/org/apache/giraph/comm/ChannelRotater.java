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

package org.apache.giraph.comm;

import java.util.Collection;
import java.util.List;
import com.google.common.collect.Lists;
import org.jboss.netty.channel.Channel;

/**
 * Maintains multiple channels and rotates between them
 */
public class ChannelRotater {
  /** Index of last used channel */
  private int index = 0;
  /** Channel list */
  private List<Channel> channelList = Lists.newArrayList();

  /**
   * Add a channel to the rotation
   *
   * @param channel Channel to add
   */
  public void addChannel(Channel channel) {
    channelList.add(channel);
  }

  /**
   * Get the next channel
   *
   * @return Next channel
   */
  public Channel nextChannel() {
    if (channelList.isEmpty()) {
      throw new IllegalArgumentException("nextChannel: No channels exist!");
    }

    incrementIndex();
    return channelList.get(index);
  }

  /**
   * Remove the last channel that was given out
   *
   * @return Return the removed channel
   */
  public Channel removeLast() {
    Channel channel = channelList.remove(index);
    incrementIndex();
    return channel;
  }

  /**
   * Increment the channel index with wrapping
   */
  private void incrementIndex() {
    ++index;
    if (index >= channelList.size()) {
      index = 0;
    }
  }

  /**
   * Get the channels
   *
   * @return Collection of the channels
   */
  Collection<Channel> getChannels() {
    return channelList;
  }
}
