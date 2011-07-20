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

package org.apache.giraph.graph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Defines a vertex index range and assigns responsibility to a particular
 * host and port.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class VertexRange<I extends WritableComparable,
                         V extends Writable,
                         E extends Writable,
                         M extends Writable> implements Writable {
    /** Current host that is responsible for this VertexRange */
    private String hostname = null;
    /** Port that the current host is using */
    private int port = -1;
    /** Previous host responsible for this {@link VertexRange} (none if empty) */
    private String previousHostname = null;
    /** Previous port (correlates to previous host), -1 if not used */
    private int previousPort = -1;
    /** Previous hostname and partition id */
    private String previousHostnameId = null;
    /** Max vertex index */
    private I maxVertexIndex = null;
    /** Hostname and partition id */
    private String hostnameId;
    /** Checkpoint file prefix (null if not recovering from a checkpoint) */
    private String checkpointfilePrefix = null;
    /** Vertex map for this range (keyed by index) */
    private final SortedMap<I, BasicVertex<I, V, E, M>> vertexMap =
        new TreeMap<I, BasicVertex<I, V, E, M>>();
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(VertexRange.class);

    @Override
    public String toString() {
        return "[maxVertexId=" + getMaxIndex() +
            ",hostname=" + getHostname() + ",port=" + getPort() +
            ",prevHostname=" + getPreviousHostname() + ",prevPort=" +
            getPreviousPort() + ",prevHostnameId=" + getPreviousHostnameId() +
            ",vertexCount=" + getVertexCount() + ",edgeCount=" + getEdgeCount() +
            ",checkpointFile=" + getCheckpointFilePrefix() + ",hostnameId" +
            getHostnameId() + "]";
    }

    public VertexRange(String hostname,
                       int port,
                       String hostnameId,
                       I maxVertexIndex,
                       String checkpointFilePrefix)
            throws InstantiationException, IllegalAccessException, IOException {
        this.hostname = hostname;
        this.port = port;
        this.hostnameId = hostnameId;
        if (maxVertexIndex != null) {
            @SuppressWarnings("unchecked")
            I newInstance = (I) maxVertexIndex.getClass().newInstance();
            maxVertexIndex = newInstance;
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(byteOutputStream);
            maxVertexIndex.write(output);
            maxVertexIndex.readFields(
                new DataInputStream(
                    new ByteArrayInputStream(byteOutputStream.toByteArray())));
        }
        checkpointfilePrefix = checkpointFilePrefix;
    }

    public VertexRange(Class<I> indexClass, JSONObject vertexRangeObj)
            throws JSONException, IOException, InstantiationException,
            IllegalAccessException {
        maxVertexIndex = indexClass.newInstance();
        byte[] maxVertexIndexByteArray =
            Base64.decodeBase64(
                vertexRangeObj.getString(
                    BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY));
        InputStream input = new ByteArrayInputStream(maxVertexIndexByteArray);
        maxVertexIndex.readFields(new DataInputStream(input));
        try {
            hostname =
                vertexRangeObj.getString(BspService.JSONOBJ_HOSTNAME_KEY);
        } catch (JSONException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("VertexRange: No hostname for " + vertexRangeObj);
            }
        }
        try {
            port = vertexRangeObj.getInt(BspService.JSONOBJ_PORT_KEY);
        } catch (JSONException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("VertexRange: No port for " + vertexRangeObj);
            }
        }

        try {
            previousHostname =
                vertexRangeObj.getString(
                    BspService.JSONOBJ_PREVIOUS_HOSTNAME_KEY);
        } catch (JSONException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("VertexRange: No previous hostname for " +
                          vertexRangeObj);
            }
        }
        try {
            previousPort =
                vertexRangeObj.getInt(BspService.JSONOBJ_PREVIOUS_PORT_KEY);
        } catch (JSONException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("VertexRange: No previous port for " +
                          vertexRangeObj);
            }
        }

        hostnameId =
            vertexRangeObj.getString(BspService.JSONOBJ_HOSTNAME_ID_KEY);
        try {
            checkpointfilePrefix =
                vertexRangeObj.getString(
                    BspService.JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY);
        } catch (JSONException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("VertexRange: No checkpoint file for " +
                          vertexRangeObj);
            }
        }
    }

    /** Copy constructor
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException */
    public VertexRange(VertexRange vertexRange)
            throws InstantiationException, IllegalAccessException, IOException {
        if (vertexRange.getHostname() != null) {
            hostname = vertexRange.getHostname();
        }
        port = vertexRange.getPort();
        if (vertexRange.getPreviousHostname() != null) {
            previousHostname = vertexRange.getPreviousHostname();
        }
        previousPort = vertexRange.getPreviousPort();
        if (vertexRange.getHostnameId() != null) {
            hostnameId = vertexRange.getHostnameId();
        }
        else {
            hostnameId = null;
        }
        @SuppressWarnings("unchecked")
        I newInstance = (I) vertexRange.getMaxIndex().getClass().newInstance();
        maxVertexIndex = newInstance;
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(byteOutputStream);
        vertexRange.getMaxIndex().write(output);
        maxVertexIndex.readFields(
            new DataInputStream(
                new ByteArrayInputStream(byteOutputStream.toByteArray())));;
        if (vertexRange.getCheckpointFilePrefix() != null) {
            checkpointfilePrefix = vertexRange.getCheckpointFilePrefix();
        }
    }

    /**
     * Get the map of vertices for this {@link VertexRange}.
     *
     * @return Map of vertices (keyed by index)
     */
    public SortedMap<I, BasicVertex<I, V, E, M>> getVertexMap() {
        return vertexMap;
    }

    public JSONObject toJSONObject() throws IOException, JSONException {
        JSONObject vertexRangeObj = new JSONObject();
        ByteArrayOutputStream outputStream =
            new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        maxVertexIndex.write(output);
        vertexRangeObj.put(BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY,
                           Base64.encodeBase64String(
                               outputStream.toByteArray()));
        vertexRangeObj.put(BspService.JSONOBJ_HOSTNAME_KEY, hostname);
        vertexRangeObj.put(BspService.JSONOBJ_PORT_KEY, port);
        vertexRangeObj.put(BspService.JSONOBJ_PREVIOUS_HOSTNAME_KEY,
                           previousHostname);
        vertexRangeObj.put(BspService.JSONOBJ_PREVIOUS_PORT_KEY,
                           previousPort);
        vertexRangeObj.put(BspService.JSONOBJ_HOSTNAME_ID_KEY, hostnameId);
        vertexRangeObj.put(BspService.JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY,
                           checkpointfilePrefix);
        return vertexRangeObj;
    }

    public final String getHostname() {
        return hostname;
    }

    public final void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public final String getPreviousHostname() {
        return previousHostname;
    }

    public final void setPreviousHostname(String previousHostname) {
        this.previousHostname = previousHostname;
    }

    public final int getPreviousPort() {
        return previousPort;
    }

    public final void setPreviousHostnameId(String previousHostnameId) {
        this.previousHostnameId = previousHostnameId;
    }

    public final String getPreviousHostnameId() {
        return previousHostnameId;
    }

    public void setPreviousPort(int previousPort) {
        this.previousPort = previousPort;
    }

    public final String getHostnameId() {
        return hostnameId;
    }

    public final void setHostnameId(String hostnameId) {
        this.hostnameId = hostnameId;
    }

    public I getMaxIndex() {
        return maxVertexIndex;
    }

    public void setMaxIndex(I index) {
        maxVertexIndex = index;
    }

    public long getVertexCount() {
        return vertexMap.size();
    }

    public long getFinishedVertexCount() {
        long finishedVertexCount = 0;
        for (BasicVertex<I, V, E, M> vertex : vertexMap.values()) {
            if (vertex.isHalted()) {
                ++finishedVertexCount;
            }
        }
        return finishedVertexCount;
    }

    public long getEdgeCount() {
        long edgeCount = 0;
        for (BasicVertex<I, V, E, M> vertex : vertexMap.values()) {
            edgeCount += vertex.getOutEdgeMap().size();
        }
        return edgeCount;
    }

    public String getCheckpointFilePrefix() {
        return checkpointfilePrefix;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        hostname = input.readUTF();
        port = input.readInt();
        previousHostname = input.readUTF();
        previousPort = input.readInt();
        hostnameId = input.readUTF();
        maxVertexIndex.readFields(input);
        checkpointfilePrefix = input.readUTF();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (hostname == null) {
            hostname = "";
        }
        if (previousHostname == null) {
            previousHostname = "";
        }
        if (hostnameId == null) {
            hostnameId = "";
        }
        if (checkpointfilePrefix == null) {
            checkpointfilePrefix = "";
        }

        output.writeUTF(hostname);
        output.writeInt(port);
        output.writeUTF(previousHostname);
        output.writeInt(previousPort);
        output.writeUTF(hostnameId);
        maxVertexIndex.write(output);
        output.writeUTF(checkpointfilePrefix);
    }
}
