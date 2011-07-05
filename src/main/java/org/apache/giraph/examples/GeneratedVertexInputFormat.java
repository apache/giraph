/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;

/**
 * This VertexInputFormat is meant for testing/debugging.  It simply generates
 * some vertex data that can be consumed by test applications. *
 */
public class GeneratedVertexInputFormat implements
    VertexInputFormat<LongWritable, IntWritable, FloatWritable> {

    public List<InputSplit> getSplits(Configuration conf, int numSplits)
        throws IOException, InterruptedException {
        /*
         * This is meaningless, the VertexReader will generate all the test
         * data.
         */
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < numSplits; ++i) {
            inputSplitList.add(new BspInputSplit(i, numSplits));
        }
        return inputSplitList;
    }

    public VertexReader<LongWritable, IntWritable, FloatWritable>
            createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new GeneratedVertexReader();
    }

}
