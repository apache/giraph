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

package org.apache.giraph.zk;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This name is used by each worker as a file to let the ZooKeeper
 * servers know that they can shutdown.
 */
@Immutable
public class ComputationDoneName {
  /** Will end the name (easy to detect if this is a name match) */
  private static final String COMPUTATION_DONE_SUFFIX =
      ".COMPUTATION_DONE";
  /** Unique worker id */
  private final int workerId;
  /** Name as a string */
  private final String name;

  /**
   * Constructor.
   *
   * @param workerId Unique worker id
   */
  public ComputationDoneName(int workerId) {
    this.workerId = workerId;
    this.name = Integer.toString(workerId) + COMPUTATION_DONE_SUFFIX;
  }

  public int getWorkerId() {
    return workerId;
  }

  public String getName() {
    return name;
  }

  /**
   * Create this object from a name (if possible).  If the name is not
   * able to be parsed this will throw various runtime exceptions.
   *
   * @param name Name to parse
   * @return ComputationDoneName object that represents this name
   */
  public static final ComputationDoneName fromName(String name) {
    checkNotNull(name, "name is null");
    checkArgument(name.endsWith(COMPUTATION_DONE_SUFFIX),
        "Name %s is not a valid ComputationDoneName", name);

    return new ComputationDoneName(
        Integer.parseInt(name.replace(COMPUTATION_DONE_SUFFIX, "")));
  }

  /**
   * Is this string a ComputationDoneName?
   *
   * @param name Name to check
   * @return True if matches the format of a ComputationDoneName,
   *         false otherwise
   */
  public static final boolean isName(String name) {
    return name.endsWith(COMPUTATION_DONE_SUFFIX);
  }
}
