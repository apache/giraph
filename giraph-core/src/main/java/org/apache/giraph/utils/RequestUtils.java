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

package org.apache.giraph.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.giraph.comm.requests.WritableRequest;

import java.io.IOException;
import org.apache.log4j.Logger;

/**
 * RequestUtils utility class
 */
public class RequestUtils {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(RequestUtils.class);

  /**
   * Private Constructor
   */
  private RequestUtils() {
  }

  /**
   * decodeWritableRequest based on predicate
   *
   * @param buf ByteBuf
   * @param request writableRequest
   * @return properly initialized writableRequest
   * @throws IOException
   */
  public static WritableRequest decodeWritableRequest(ByteBuf buf,
    WritableRequest request) throws IOException {
    ByteBufInputStream input = new ByteBufInputStream(buf);
    request.readFields(input);
    return request;
  }
}
