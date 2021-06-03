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
package org.apache.giraph.bsp.checkpoints;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.master.MasterCompute;

/**
 * To prevent accidental checkpointing of non-checkpointable app
 * you may provide implementation of this interface. Most apps are
 * checkpointable by default, however some apps are not checkpointable,
 * e.g. apps that use static variables to pass data around between supersteps
 * or start new threads or use external resources. This interface allows
 * you to specify if and when your app is checkpointable.
 */
public interface CheckpointSupportedChecker {

  /**
   * Does the job support checkpoints?
   * It is true by default, set it to false if your job uses some
   * non-checkpointable features:
   * - static variables for storing data between supersteps.
   * - starts new threads or uses Timers
   * - writes output before job is complete, etc
   * This method is called on master and has access to
   * job configuration and master compute.
   *
   * @param conf giraph configuration
   * @param masterCompute instance of master compute
   * @return true if checkpointing on current superstep is supported
   * by this application.
   */
  boolean isCheckpointSupported(GiraphConfiguration conf,
                                       MasterCompute masterCompute);

}
