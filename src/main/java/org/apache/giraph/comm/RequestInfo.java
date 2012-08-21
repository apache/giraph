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

import java.net.InetSocketAddress;
import java.util.Date;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jboss.netty.channel.ChannelFuture;

/**
 * Help track requests throughout the system
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class RequestInfo<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /** Destination of the request */
  private final InetSocketAddress destinationAddress;
  /** When the request was started */
  private final long startedMsecs;
  /** Request */
  private final WritableRequest<I, V, E, M> request;
  /** Future of the write of this request*/
  private volatile ChannelFuture writeFuture;

  /**
   * Constructor.
   *
   * @param destinationAddress Destination of the request
   * @param request Request that is sent
   */
  public RequestInfo(InetSocketAddress destinationAddress,
                     WritableRequest<I, V, E, M> request) {
    this.destinationAddress = destinationAddress;
    this.request = request;
    this.startedMsecs = System.currentTimeMillis();
  }

  public InetSocketAddress getDestinationAddress() {
    return destinationAddress;
  }

  public long getStartedMsecs() {
    return startedMsecs;
  }

  /**
   * Get the elapsed time since the request started.
   *
   * @return Msecs since the request was started
   */
  public long getElapsedMsecs() {
    return System.currentTimeMillis() - startedMsecs;
  }

  public WritableRequest<I, V, E, M> getRequest() {
    return request;
  }

  public void setWriteFuture(ChannelFuture writeFuture) {
    this.writeFuture = writeFuture;
  }

  public ChannelFuture getWriteFuture() {
    return writeFuture;
  }

  @Override
  public String toString() {
    return "(destAddr=" + destinationAddress +
        ",startDate=" + new Date(startedMsecs) + ",elapsedMsecs=" +
        getElapsedMsecs() + ",reqId=" + request.getRequestId() +
        ((writeFuture == null) ? ")" :
            ",writeDone=" + writeFuture.isDone() +
                ",writeSuccess=" + writeFuture.isSuccess() + ")");
  }
}
