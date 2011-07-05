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
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.graph.VertexCombiner;

/**
 * Test whether combiner is called by summing up the messages.
 */
public class SimpleSumCombiner
        implements VertexCombiner<LongWritable, IntWritable,
        FloatWritable, IntWritable> {

    @Override
    public IntWritable combine(LongWritable vertexIndex,
                               List<IntWritable> msgList)
            throws IOException {
        int sum = 0;
        for (IntWritable msg : msgList) {
            sum += msg.get();
        }
        return new IntWritable(sum);
    }
}
