package com.yahoo.hadoop_bsp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Defines a vertex index range and assigns responsibility to a particular
 * host and port.
 * @author aching
 *
 * @param <I> vertex index type
 */
@SuppressWarnings("rawtypes")
public class VertexRange<I extends WritableComparable>
    implements Comparable<VertexRange<I>> {
    /** Host that is responsible for this VertexRange */
    private String m_hostname = null;
    /** Hostname and partition id */
    private final String m_hostnameId;
    /** Port that the host is using */
    private int m_port = -1;
    /** Maximum vertex index of this VertexRange */
    private I m_maxVertexIndex;
    /** Number of vertices in this VertexRange (from the previous superstep) */
    private final long m_vertexCount;
    /** Number of edges in this VertexRange (from the previous superstep) */
    private final long m_edgeCount;
    /** Checkpoint file prefix (null if not recovering from a checkpoint) */
    private String m_checkpointfilePrefix = null;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(VertexRange.class);

    VertexRange(String hostname,
                int port,
                String hostnameId,
                I maxVertexIndex,
                long vertexCount,
                long edgeCount,
                String checkpointFilePrefix) {
        m_hostname = hostname;
        m_hostnameId = hostnameId;
        m_port = port;
        m_maxVertexIndex = maxVertexIndex;
        m_vertexCount = vertexCount;
        m_edgeCount = edgeCount;
        m_checkpointfilePrefix = checkpointFilePrefix;
    }

    VertexRange(I maxVertexIndex, JSONObject vertexRangeObj)
        throws JSONException, IOException {
        m_maxVertexIndex = maxVertexIndex;
        InputStream input =
            new ByteArrayInputStream(
                vertexRangeObj.getString(
                    BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY).getBytes("UTF-8"));
        m_maxVertexIndex.readFields(new DataInputStream(input));
        try {
            m_hostname =
                vertexRangeObj.getString(BspService.JSONOBJ_HOSTNAME_KEY);
        } catch (JSONException e) {
            LOG.debug("VertexRange: No hostname for " + vertexRangeObj);
        }
        try {
            m_port = vertexRangeObj.getInt(BspService.JSONOBJ_PORT_KEY);
        } catch (JSONException e) {
            LOG.debug("VertexRange: No port for " + vertexRangeObj);
        }
        m_hostnameId =
            vertexRangeObj.getString(BspService.JSONOBJ_HOSTNAME_ID_KEY);
        m_vertexCount =
            vertexRangeObj.getLong(BspService.JSONOBJ_NUM_VERTICES_KEY);
        m_edgeCount = vertexRangeObj.getLong(BspService.JSONOBJ_NUM_EDGES_KEY);
        try {
            m_checkpointfilePrefix =
                vertexRangeObj.getString(
                    BspService.JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY);
        } catch (JSONException e) {
            LOG.debug("VertexRange: No checkpoint file for " + vertexRangeObj);
        }
    }

    public JSONObject toJSONObject() throws IOException, JSONException {
        JSONObject vertexRangeObj = new JSONObject();
        ByteArrayOutputStream outputStream =
            new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        m_maxVertexIndex.write(output);
        vertexRangeObj.put(BspService.JSONOBJ_MAX_VERTEX_INDEX_KEY,
                           outputStream.toString("UTF-8"));
        vertexRangeObj.put(BspService.JSONOBJ_HOSTNAME_KEY, m_hostname);
        vertexRangeObj.put(BspService.JSONOBJ_PORT_KEY, m_port);
        vertexRangeObj.put(BspService.JSONOBJ_HOSTNAME_ID_KEY, m_hostnameId);
        vertexRangeObj.put(BspService.JSONOBJ_NUM_VERTICES_KEY, m_vertexCount);
        vertexRangeObj.put(BspService.JSONOBJ_NUM_EDGES_KEY, m_edgeCount);
        vertexRangeObj.put(BspService.JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY,
                           m_checkpointfilePrefix);
        return vertexRangeObj;
    }

    public final String getHostname() {
        return new String(m_hostname);
    }

    public final String getHostnameId() {
        return new String(m_hostnameId);
    }

    public int getPort() {
        return m_port;
    }

    public I getMaxIndex() {
        return m_maxVertexIndex;
    }

    public void setMaxIndex(I index) {
        m_maxVertexIndex = index;
    }

    long getVertexCount() {
        return m_vertexCount;
    }

    long getEdgeCount() {
        return m_edgeCount;
    }

    String getCheckpointFilePrefix() {
        return m_checkpointfilePrefix;
    }

    public int compareTo(VertexRange<I> otherObject) {
        @SuppressWarnings("unchecked")
        int compareTo =
            m_maxVertexIndex.compareTo(((VertexRange<I>) otherObject).getMaxIndex());
        if (compareTo != 0) {
            return compareTo;
        }
        if (getVertexCount() < otherObject.getVertexCount()) {
            return -1;
        }
        else if (getVertexCount() > otherObject.getVertexCount()) {
            return 1;
        }
        if (getEdgeCount() < otherObject.getEdgeCount()) {
            return -1;
        }
        else if (getEdgeCount() < otherObject.getEdgeCount()) {
            return 1;
        }
        return 0;
    }
}
