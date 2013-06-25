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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.hadoop.io.Writable;

/**
 * Special iterable that recycles the message
 *
 * @param <M> Message data
 */
public class MessagesIterable<M extends Writable>
    extends RepresentativeByteArrayIterable<M> {
  /** Message class */
  private final Class<M> messageClass;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param messageClass Message class
   * @param buf Buffer
   * @param off Offset to start in the buffer
   * @param length Length of the buffer
   */
  public MessagesIterable(
      ImmutableClassesGiraphConfiguration conf, Class<M> messageClass,
      byte[] buf, int off, int length) {
    super(conf, buf, off, length);
    this.messageClass = messageClass;
  }

  @Override
  protected M createWritable() {
    return ReflectionUtils.newInstance(messageClass);
  }
}
