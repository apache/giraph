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

package org.apache.giraph.io.formats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * Utility to access file output format in a
 * way independent of current hadoop implementation.
 */
public class FileOutputFormatUtil {

  /**
   * Private constructor for utility class.
   */
  private FileOutputFormatUtil() { }

  /**
   * Set the Path of the output directory for the map-reduce job.
   * @param job The job to modify
   * @param outputDir the Path of the output directory for the map-reduce job.
   */
  public static void setOutputPath(Job job, Path outputDir) {
    job.getConfiguration().set("mapred.output.dir", outputDir.toString());
  }

}
