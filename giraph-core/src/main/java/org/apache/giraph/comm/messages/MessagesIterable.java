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

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.Factory;
import org.apache.giraph.utils.RepresentativeByteStructIterable;
import org.apache.hadoop.io.Writable;

/**
 * Special iterable that recycles the message
 *
 * @param <M> Message data
 */
public class MessagesIterable<M extends Writable>
    extends RepresentativeByteStructIterable<M> {
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor
   *
   * @param dataInputFactory Factory for data inputs
   * @param messageValueFactory factory for creating message values
   */
  public MessagesIterable(
      Factory<? extends ExtendedDataInput> dataInputFactory,
      MessageValueFactory<M> messageValueFactory) {
    super(dataInputFactory);
    this.messageValueFactory = messageValueFactory;
  }

  @Override
  protected M createWritable() {
    return messageValueFactory.newInstance();
  }
}
