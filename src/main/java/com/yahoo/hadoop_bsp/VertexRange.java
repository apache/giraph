package com.yahoo.hadoop_bsp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Defines a vertex index range and assigns responsibility to a particular
 * host and port.
 *
 * @param <I> vertex index type
 */
@SuppressWarnings("rawtypes")
public class VertexRange<I extends WritableComparable,
                         V extends Writable,
                         E extends Writable,
                         M extends Writable> implements Writable {
    /** Current host that is responsible for this VertexRange */
    private String m_hostname = null;
    /** Port that the current host is using */
    private int m_port = -1;
    /** Previous host responsible for this {@link VertexRange} (none if empty) */
    private String m_previousHostname = null;
    /** Previous port (correlates to previous host), -1 if not used */
    private int m_previousPort = -1;
    /** Max vertex index */
    private I m_maxVertexIndex = null;
    /** Hostname and partition id */
    private String m_hostnameId = new String();
    /** Number of vertices in this VertexRange (from the previous superstep) */
    private long m_vertexCount = -1;
    /** Number of edges in this VertexRange (from the previous superstep) */
    private long m_edgeCount = -1;
    /** Checkpoint file prefix (null if not recovering from a checkpoint) */
    private String m_checkpointfilePrefix = null;
    /** Vertices (sorted) for this range */
    private final List<Vertex<I, V, E, M>> m_vertexList =
        new ArrayList<Vertex<I, V, E, M>>();
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(VertexRange.class);

    @Override
    public String toString() {
        String retString = new String();
        retString += "[hostname=" + getHostname() + ",port=" + getPort() +
            ",prevHostname=" + getPreviousHostname() + ",prevPort=" +
            getPreviousPort() + ",vertexCount" +
            getVertexCount() + ",edgeCount=" + getEdgeCount() +
            ",checkpointFile=" + getCheckpointFilePrefix() + ",hostnameId" +
            getHostnameId() + "]";
        return retString;
    }

    VertexRange(String hostname,
                int port,
                String hostnameId,
                I maxVertexIndex,
                long vertexCount,
                long edgeCount,
                String checkpointFilePrefix)
            throws InstantiationException, IllegalAccessException, IOException {
        m_hostname = hostname;
        m_port = port;
        m_hostnameId = hostnameId;
        if (maxVertexIndex != null) {
            @SuppressWarnings("unchecked")
            I newInstance = (I) maxVertexIndex.getClass().newInstance();
            m_maxVertexIndex = newInstance;
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(byteOutputStream);
            maxVertexIndex.write(output);
            m_maxVertexIndex.readFields(
                new DataInputStream(
                    new ByteArrayInputStream(byteOutputStream.toByteArray())));
        }
        m_vertexCount = vertexCount;
        m_edgeCount = edgeCount;
        m_checkpointfilePrefix = checkpointFilePrefix;
    }

    VertexRange(Class<I> indexClass, JSONObject vertexRangeObj)
            throws JSONException, IOException, InstantiationException,
                   IllegalAccessException {
        I newInstance = indexClass.newInstance();
        m_maxVertexIndex = newInstance;
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

        try {
            m_previousHostname =
                vertexRangeObj.getString(
                    BspService.JSONOBJ_PREVIOUS_HOSTNAME_KEY);
        } catch (JSONException e) {
            LOG.debug("VertexRange: No previous hostname for " +
                      vertexRangeObj);
        }
        try {
            m_previousPort =
                vertexRangeObj.getInt(BspService.JSONOBJ_PREVIOUS_PORT_KEY);
        } catch (JSONException e) {
            LOG.debug("VertexRange: No previous port for " + vertexRangeObj);
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

    /** Copy constructor
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException */
    public VertexRange(VertexRange vertexRange)
            throws InstantiationException, IllegalAccessException, IOException {
        if (vertexRange.getHostname() != null) {
            m_hostname = new String(vertexRange.getHostname());
        }
        m_port = vertexRange.getPort();
        if (vertexRange.getPreviousHostname() != null) {
            m_previousHostname = new String(vertexRange.getPreviousHostname());
        }
        m_previousPort = vertexRange.getPreviousPort();
        if (vertexRange.getHostnameId() != null) {
            m_hostnameId = new String(vertexRange.getHostnameId());
        }
        else {
            m_hostnameId = null;
        }
        @SuppressWarnings("unchecked")
        I newInstance = (I) vertexRange.getMaxIndex().getClass().newInstance();
        m_maxVertexIndex = newInstance;
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(byteOutputStream);
        vertexRange.getMaxIndex().write(output);
        m_maxVertexIndex.readFields(
            new DataInputStream(
                new ByteArrayInputStream(byteOutputStream.toByteArray())));;
        m_vertexCount = vertexRange.getVertexCount();
        m_edgeCount = vertexRange.getEdgeCount();
        if (vertexRange.getCheckpointFilePrefix() != null) {
            m_checkpointfilePrefix =
                new String(vertexRange.getCheckpointFilePrefix());
        }
    }

    public List<Vertex<I, V, E, M>> getVertexList() {
        return m_vertexList;
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
        vertexRangeObj.put(BspService.JSONOBJ_PREVIOUS_HOSTNAME_KEY,
                           m_previousHostname);
        vertexRangeObj.put(BspService.JSONOBJ_PREVIOUS_PORT_KEY,
                           m_previousPort);
        vertexRangeObj.put(BspService.JSONOBJ_HOSTNAME_ID_KEY, m_hostnameId);
        vertexRangeObj.put(BspService.JSONOBJ_NUM_VERTICES_KEY, m_vertexCount);
        vertexRangeObj.put(BspService.JSONOBJ_NUM_EDGES_KEY, m_edgeCount);
        vertexRangeObj.put(BspService.JSONOBJ_CHECKPOINT_FILE_PREFIX_KEY,
                           m_checkpointfilePrefix);
        return vertexRangeObj;
    }

    public final String getHostname() {
        return m_hostname;
    }

    public final void setHostname(String hostname) {
        m_hostname = hostname;
    }

    public int getPort() {
        return m_port;
    }

    public void setPort(int port) {
        m_port = port;
    }

    public final String getPreviousHostname() {
        return m_previousHostname;
    }

    public final void setPreviousHostname(String previousHostname) {
        m_previousHostname = previousHostname;
    }

    public int getPreviousPort() {
        return m_previousPort;
    }

    public void setPreviousPort(int previousPort) {
        m_previousPort = previousPort;
    }

    public final String getHostnameId() {
        return m_hostnameId;
    }

    public final void setHostnameId(String hostnameId) {
        m_hostnameId = hostnameId;
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

    void resetVertexCount() {
        m_vertexCount = 0;
    }

    long getEdgeCount() {
        return m_edgeCount;
    }

    void resetEdgeCount() {
        m_edgeCount = 0;
    }

    String getCheckpointFilePrefix() {
        return m_checkpointfilePrefix;
    }

    public void readFields(DataInput input) throws IOException {
        m_hostname = input.readUTF();
        m_port = input.readInt();
        m_previousHostname = input.readUTF();
        m_previousPort = input.readInt();
        m_hostnameId = input.readUTF();
        m_maxVertexIndex.readFields(input);
        m_vertexCount = input.readLong();
        m_edgeCount = input.readLong();
        m_checkpointfilePrefix = input.readUTF();
    }

    public void write(DataOutput output) throws IOException {
        if (m_hostname == null) {
            m_hostname = new String();
        }
        if (m_previousHostname == null) {
            m_previousHostname = new String();
        }
        if (m_hostnameId == null) {
            m_hostnameId = new String();
        }
        if (m_checkpointfilePrefix == null) {
            m_checkpointfilePrefix = new String();
        }

        output.writeUTF(m_hostname);
        output.writeInt(m_port);
        output.writeUTF(m_previousHostname);
        output.writeInt(m_previousPort);
        output.writeUTF(m_hostnameId);
        m_maxVertexIndex.write(output);
        output.writeLong(m_vertexCount);
        output.writeLong(m_edgeCount);
        output.writeUTF(m_checkpointfilePrefix);
    }
}
