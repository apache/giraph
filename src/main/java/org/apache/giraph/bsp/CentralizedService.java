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

package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic service interface shared by both {@link CentralizedServiceMaster} and
 * {@link CentralizedServiceWorker}.
 */
@SuppressWarnings("rawtypes")
public interface CentralizedService<I extends WritableComparable,
                                    V extends Writable,
                                    E extends Writable,
                                    M extends Writable> {
    /**
     * Setup (must be called prior to any other function)
     */
    void setup();

    /**
     * Get the representative Vertex for this worker.  It can used to
     * call pre/post application/superstep methods defined by the user.
     *
     * @return representation vertex
     */
    BasicVertex<I, V, E, M> getRepresentativeVertex();

    /**
     * Get the current global superstep of the application to work on.
     *
     * @return global superstep (begins at -1)
     */
    long getSuperstep();

    /**
     * Get the restarted superstep
     *
     * @return -1 if not manually restarted, otherwise the superstep id
     */
    long getRestartedSuperstep();

    /**
     * Given a superstep, should it be checkpointed based on the
     * checkpoint frequency?
     *
     * @param superstep superstep to check against frequency
     * @return true if checkpoint frequency met or superstep is 1.
     */
    boolean checkpointFrequencyMet(long superstep);

    /**
     * Clean up the service (no calls may be issued after this)
     */
    void cleanup() throws IOException;
}
