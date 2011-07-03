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

package org.apache.giraph.comm;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Interface for message communication server
 */
@SuppressWarnings("rawtypes")
public interface ServerInterface<I extends WritableComparable,
                                 V extends Writable,
                                 E extends Writable,
                                 M extends Writable>
                                 extends Closeable,
                                 WorkerCommunications<I, V, E, M> {

    /**
     * Move the in transition messages to the in messages for every vertex and
     * add new connections to any newly appearing RPC proxies.
     */
    void prepareSuperstep();

    /**
     * Flush all outgoing messages.  This will synchronously ensure that all
     * messages have been send and delivered prior to returning.
     *
     * @throws IOException
     */
    void flush(Mapper<?, ?, ?, ?>.Context context) throws IOException;

    /**
     * Closes all connections.
     *
     * @throws IOException
     */
    void closeConnections() throws IOException;

    /**
     * Shuts down.
     */
    void close();
}
