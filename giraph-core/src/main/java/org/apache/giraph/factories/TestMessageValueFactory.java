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
package org.apache.giraph.factories;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;

/**
 * Message Factory class that allows setting the message value class
 *
 * @param <M> Message Value class
 */
public class TestMessageValueFactory<M extends Writable>
    implements MessageValueFactory<M> {
  /** The Message Value class */
  private final Class<M> klass;

  /**
   * Constructor
   *
   * @param klass Message value class
   */
  public TestMessageValueFactory(Class<M> klass) {
    this.klass = klass;
  }

  @Override public M newInstance() {
    return ReflectionUtils.newInstance(klass);
  }
}
