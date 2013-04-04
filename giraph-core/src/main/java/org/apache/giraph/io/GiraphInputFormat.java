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

package org.apache.giraph.io;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Common interface for {@link VertexInputFormat} and {@link EdgeInputFormat}.
 */
public interface GiraphInputFormat {
  /**
   * Get the list of input splits for the format.
   *
   * @param context The job context
   * @param minSplitCountHint Minimum number of splits to create (hint)
   * @return The list of input splits
   * @throws IOException
   * @throws InterruptedException
   */
  List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException;
}
