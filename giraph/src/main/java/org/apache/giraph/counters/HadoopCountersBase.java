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

package org.apache.giraph.counters;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Base class for groups of Hadoop Counters.
 */
public abstract class HadoopCountersBase
    implements Iterable<GiraphHadoopCounter> {
  /** Hadoop Context used to create Counters */
  private final Context context;
  /** group to put counters under */
  private final String groupName;

  /**
   * Initialize with Hadoop Context and group name.
   *
   * @param context Hadoop Context to use.
   * @param groupName String group name to use.
   */
  protected HadoopCountersBase(Context context, String groupName) {
    this.context = context;
    this.groupName = groupName;
  }

  /**
   * Get Hadoop Context
   *
   * @return Context object used by Hadoop
   */
  public Context getContext() {
    return context;
  }

  /**
   * Get or create counter with given name and class's group name.
   *
   * @param name String name of counter
   * @return GiraphHadoopCounter found or created
   */
  protected GiraphHadoopCounter getCounter(String name) {
    return new GiraphHadoopCounter(context.getCounter(groupName, name));
  }
}
