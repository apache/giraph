/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.graph.Vertex;

/**
 * Test whether messages can go through a combiner.
 *
 */
public class SimpleCombinerVertex extends
    Vertex<LongWritable, IntWritable, FloatWritable, IntWritable> {
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getVertexId().equals(new LongWritable(2))) {
            sendMsg(new LongWritable(1), new IntWritable(101));
            sendMsg(new LongWritable(1), new IntWritable(102));
            sendMsg(new LongWritable(1), new IntWritable(103));
        }
        if (!getVertexId().equals(new LongWritable(1))) {
            voteToHalt();
        }
        else {
            // Check the messages
            int sum = 0;
            int num = 0;
            while (msgIterator != null && msgIterator.hasNext()) {
                sum += msgIterator.next().get();
                num++;
            }
            System.out.println("TestCombinerVertex: Received a sum of " + sum +
            " (should have 306 with a single message value)");

            if (num == 1 && sum == 306) {
                voteToHalt();
            }
        }
        if (getSuperstep() > 3) {
            throw new IllegalStateException(
                "TestCombinerVertex: Vertex 1 failed to receive " +
                "messages in time");
        }
    }

    public IntWritable createMsgValue() {
        return new IntWritable(0);
    }
}
