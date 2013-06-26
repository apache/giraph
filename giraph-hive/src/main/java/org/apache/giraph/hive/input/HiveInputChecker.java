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
package org.apache.giraph.hive.input;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * Interface to check the validity of a Hive input configuration.
 */
public interface HiveInputChecker {
  /**
   * Check the input is valid. This method provides information to the user as
   * early as possible so that they may validate they are using the correct
   * input and fail the job early rather than getting into it and waiting a long
   * time only to find out something was misconfigured.
   *
   * @param inputDesc HiveInputDescription
   * @param schema Hive table schema
   */
  void checkInput(HiveInputDescription inputDesc, HiveTableSchema schema);
}
