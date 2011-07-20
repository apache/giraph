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
import org.apache.log4j.Logger;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class SimpleShortestPathsVertex extends
        Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SimpleShortestPathsVertex.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;

    @Override
    public void preApplication()
            throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
    }

    @Override
    public void preSuperstep() {
    }

    /**
     * Is this vertex the source id?
     *
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() ==
            getContext().getConfiguration().getLong(SOURCE_ID,
                                                    SOURCE_ID_DEFAULT));
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        double minDist = isSource() ? 0d : Double.MAX_VALUE;
        while (msgIterator.hasNext()) {
            minDist = Math.min(minDist, msgIterator.next().get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + getVertexId() + " got minDist = " + minDist);
        }
        if (minDist < getVertexValue().get()) {
            setVertexValue(new DoubleWritable(minDist));
            for (Edge<LongWritable, FloatWritable> edge :
                    getOutEdgeMap().values()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + getVertexId() + " sent to " +
                              edge.getDestVertexId() + " = " +
                              minDist + edge.getEdgeValue());
                }
                sendMsg(edge.getDestVertexId(),
                        new DoubleWritable(minDist + edge.getEdgeValue().get()));
            }
        }
        voteToHalt();
    }
}
