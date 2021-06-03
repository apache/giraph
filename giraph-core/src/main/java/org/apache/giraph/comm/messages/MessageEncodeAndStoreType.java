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

package org.apache.giraph.comm.messages;

/**
 * There are two types of message-stores currently
 * pointer based, and default byte-array based
 */
public enum MessageEncodeAndStoreType {
  /**
   * Use a byte-array to store messages for each partition
   */
  BYTEARRAY_PER_PARTITION(false),
  /**
   * Extract a byte array per partition from one message to many ids encoding
   * and then store
   */
  EXTRACT_BYTEARRAY_PER_PARTITION(true),
  /**
   * Use message-store which is based on list of pointers to encoded messages
   */
  POINTER_LIST_PER_VERTEX(true);

  /** Can use one message to many ids encoding? */
  private final boolean oneMessageToManyIdsEncoding;

  /**
   * Constructor
   *
   * @param oneMessageToManyIdsEncoding use one message to many ids encoding
   */
  MessageEncodeAndStoreType(boolean oneMessageToManyIdsEncoding) {
    this.oneMessageToManyIdsEncoding = oneMessageToManyIdsEncoding;
  }

  /**
   * True if one message to many ids encoding is set
   * @return return oneMessageToManyIdsEncoding
   */
  public boolean useOneMessageToManyIdsEncoding() {
    return oneMessageToManyIdsEncoding;
  }
}
