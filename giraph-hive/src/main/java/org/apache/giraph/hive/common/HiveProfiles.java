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

package org.apache.giraph.hive.common;

/**
 * Profiles used throughout Hive code.
 */
public class HiveProfiles {
  /** name of vertex input profile */
  public static final String VERTEX_INPUT_PROFILE_ID = "vertex_input_profile";
  /** name of edge input profile */
  public static final String EDGE_INPUT_PROFILE_ID = "edge_input_profile";
  /** Name of vertex output profile */
  public static final String VERTEX_OUTPUT_PROFILE_ID = "vertex_output_profile";

  /** Disable creation */
  private HiveProfiles() { }
}
