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

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.graph.Vertex;

/**
 * Vertex to allow unit testing of failure detection
 */
public class SimpleFailVertex extends
    Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    static long superstep = 0;

    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            setVertexValue(vertexValue);
            if (getSuperstep() < 30) {
                if (getSuperstep() == 20) {
                    if (getVertexId().get() == 10L) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                        }
                        System.exit(1);
                    } else if (getSuperstep() - superstep > 10) {
                        return;
                    }
                }
                long edges = getOutEdgeMap().size();
                sentMsgToAllEdges(
                    new DoubleWritable(getVertexValue().get() / edges));
            } else {
                voteToHalt();
            }
            superstep = getSuperstep();
        }
    }

    public DoubleWritable createMsgValue() {
        return new DoubleWritable(0f);
    }
}
