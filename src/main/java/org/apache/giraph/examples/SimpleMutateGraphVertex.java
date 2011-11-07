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

package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.WorkerContext;

/**
 * Vertex to allow unit testing of graph mutations.
 */
public class SimpleMutateGraphVertex extends
        Vertex<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable> {
    /** Maximum number of ranges for vertex ids */
    private long maxRanges = 100;
    /** Class logger */
    private static Logger LOG =
        Logger.getLogger(SimpleMutateGraphVertex.class);

    /**
     * Unless we create a ridiculous number of vertices , we should not
     * collide within a vertex range defined by this method.
     *
     * @return Starting vertex id of the range
     */
    private long rangeVertexIdStart(int range) {
        return (Long.MAX_VALUE / maxRanges) * range;
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) throws IOException {

    	SimpleMutateGraphVertexWorkerContext workerContext =
    		(SimpleMutateGraphVertexWorkerContext) getWorkerContext();

    	if (getSuperstep() == 1) {
            // Send messages to vertices that are sure not to exist
            // (creating them)
            LongWritable destVertexId =
                new LongWritable(rangeVertexIdStart(1) + getVertexId().get());
            sendMsg(destVertexId, new DoubleWritable(0.0));
        } else if (getSuperstep() == 2) {
        } else if (getSuperstep() == 3) {
        	long vertex_count = workerContext.getVertexCount();
            if (vertex_count * 2 != getNumVertices()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumVertices() +
                    " vertices when should have " + vertex_count * 2 +
                    " on superstep " + getSuperstep());
            }
            long edge_count = workerContext.getEdgeCount();
            if (edge_count != getNumEdges()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumEdges() +
                    " edges when should have " + edge_count +
                    " on superstep " + getSuperstep());
            }
            // Create vertices that are sure not to exist (doubling vertices)
            MutableVertex<LongWritable, DoubleWritable,
                FloatWritable, DoubleWritable> vertex = instantiateVertex();
            LongWritable vertexIndex =
                new LongWritable(rangeVertexIdStart(3) + getVertexId().get());
            vertex.setVertexId(vertexIndex);
            addVertexRequest(vertex);
            // Add edges to those remote vertices as well
            addEdgeRequest(vertexIndex,
                           new Edge<LongWritable, FloatWritable>(
                               getVertexId(), new FloatWritable(0.0f)));
        } else if (getSuperstep() == 4) {
        } else if (getSuperstep() == 5) {
        	long vertex_count = workerContext.getVertexCount();
            if (vertex_count * 2 != getNumVertices()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumVertices() +
                    " when should have " + vertex_count * 2 +
                    " on superstep " + getSuperstep());
            }
            long edge_count = workerContext.getEdgeCount();
            if (edge_count + vertex_count != getNumEdges()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumEdges() +
                    " edges when should have " + edge_count + vertex_count +
                    " on superstep " + getSuperstep());
            }
            // Remove the edges created in superstep 3
            LongWritable vertexIndex =
                new LongWritable(rangeVertexIdStart(3) + getVertexId().get());
            workerContext.increaseEdgesRemoved();
            removeEdgeRequest(vertexIndex, getVertexId());
        } else if (getSuperstep() == 6) {
            // Remove all the vertices created in superstep 3
            if (getVertexId().compareTo(
                    new LongWritable(rangeVertexIdStart(3))) >= 0) {
                removeVertexRequest(getVertexId());
            }
        } else if (getSuperstep() == 7) {
        	long orig_edge_count = workerContext.getOrigEdgeCount();
            if (orig_edge_count != getNumEdges()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumEdges() +
                    " edges when should have " + orig_edge_count +
                    " on superstep " + getSuperstep());
            }
        } else if (getSuperstep() == 8) {
        	long vertex_count = workerContext.getVertexCount();
            if (vertex_count / 2 != getNumVertices()) {
                throw new IllegalStateException(
                    "Impossible to have " + getNumVertices() +
                    " vertices when should have " + vertex_count / 2 +
                    " on superstep " + getSuperstep());
            }
        }
        else {
            voteToHalt();
        }
    }

    public static class SimpleMutateGraphVertexWorkerContext extends WorkerContext {
        /** Cached vertex count */
        private long vertexCount;
        /** Cached edge count */
        private long edgeCount;
        /** Original number of edges */
        private long origEdgeCount;
        /** Number of edges removed during superstep */
        private int edgesRemoved = 0;

		@Override
		public void preApplication()
				throws InstantiationException, IllegalAccessException { }

		@Override
		public void postApplication() { }

		@Override
		public void preSuperstep() {
			vertexCount = getNumVertices();
			edgeCount = getNumEdges();
			if (getSuperstep() == 1) {
				origEdgeCount = edgeCount;
			}
			LOG.info("Got " + vertexCount + " vertices, " +
					 edgeCount + " edges on superstep " +
					 getSuperstep());
			LOG.info("Removed " + edgesRemoved);
			edgesRemoved = 0;
		}

		@Override
		public void postSuperstep() { }

		public long getVertexCount() {
			return vertexCount;
		}

		public long getEdgeCount() {
			return edgeCount;
		}

		public long getOrigEdgeCount() {
			return origEdgeCount;
		}

		public void increaseEdgesRemoved() {
			this.edgesRemoved++;
		}
    }
}
