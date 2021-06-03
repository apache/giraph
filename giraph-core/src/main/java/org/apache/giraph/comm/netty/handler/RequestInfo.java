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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.Date;

/**
 * Help track requests throughout the system
 */
public class RequestInfo {
  /** Time class to use */
  private static final Time TIME = SystemTime.get();
  /** Destination of the request */
  private final InetSocketAddress destinationAddress;
  /** When the request was started */
  private final long startedNanos;
  /** Request */
  private final WritableRequest request;
  /** Future of the write of this request*/
  private volatile ChannelFuture writeFuture;

  /**
   * Constructor.
   *
   * @param destinationAddress Destination of the request
   * @param request Request that is sent
   */
  public RequestInfo(InetSocketAddress destinationAddress,
                     WritableRequest request) {
    this.destinationAddress = destinationAddress;
    this.request = request;
    this.startedNanos = TIME.getNanoseconds();
  }

  public InetSocketAddress getDestinationAddress() {
    return destinationAddress;
  }

  /**
   * Get the started msecs.
   *
   * @return Started msecs
   */
  public long getStartedMsecs() {
    return startedNanos / Time.NS_PER_MS;
  }

  /**
   * Get the elapsed nanoseconds since the request started.
   *
   * @return Nanoseconds since the request was started
   */
  public long getElapsedNanos() {
    return TIME.getNanoseconds() - startedNanos;
  }

  /**
   * Get the elapsed millseconds since the request started.
   *
   * @return Milliseconds since the request was started
   */
  public long getElapsedMsecs() {
    return getElapsedNanos() / Time.NS_PER_MS;
  }


  public WritableRequest getRequest() {
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
    return "(reqId=" + request.getRequestId() +
        ",destAddr=" + destinationAddress.getHostName() + ":" +
        destinationAddress.getPort() +
        ",elapsedNanos=" +
        getElapsedNanos() +
        ",started=" + new Date(getStartedMsecs()) +
        ((writeFuture == null) ? ")" :
            ",writeDone=" + writeFuture.isDone() +
                ",writeSuccess=" + writeFuture.isSuccess() + ")");
  }
}
