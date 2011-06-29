package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.VertexReader;

/**
 * Used by TestVertexInputFormat to read some generated data
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public class GeneratedVertexReader implements
    VertexReader<LongWritable, IntWritable, FloatWritable> {
    /** Logger */
    private static final Logger LOG =
        Logger.getLogger(GeneratedVertexReader.class);
    /** Records read so far */
    long m_recordsRead = 0;
    /** Total records to read (on this split alone) */
    long m_totalRecords = 0;
    /** The input split from initialize(). */
    BspInputSplit m_inputSplit = null;

    public static final String READER_VERTICES =
        "TestVertexReader.reader_vertices";
    public static final long DEFAULT_READER_VERTICES = 10;

    public void initialize(
        InputSplit inputSplit, TaskAttemptContext context)
        throws IOException {
        Configuration configuration = context.getConfiguration();
            m_totalRecords = configuration.getLong(
                GeneratedVertexReader.READER_VERTICES,
                GeneratedVertexReader.DEFAULT_READER_VERTICES);
            m_inputSplit = (BspInputSplit) inputSplit;
    }

    public boolean next(
            MutableVertex<LongWritable, IntWritable, FloatWritable, ?> vertex)
            throws IOException {
        if (m_totalRecords <= m_recordsRead) {
            return false;
        }
        vertex.setVertexId(new LongWritable(
            (m_inputSplit.getSplitIndex() * m_totalRecords) + m_recordsRead));
        vertex.setVertexValue(
            new IntWritable(((int) (vertex.getVertexId().get() * 10))));
        long destVertexId =
            (vertex.getVertexId().get() + 1) %
            (m_inputSplit.getNumSplits() * m_totalRecords);
        float edgeValue = (float) vertex.getVertexId().get() * 100;
        // Adds an edge to the neighbor vertex

        vertex.addEdge(new Edge<LongWritable, FloatWritable>(
            new LongWritable(destVertexId),
            new FloatWritable(edgeValue)));
        ++m_recordsRead;
        LOG.info("next: Return vertexId=" + vertex.getVertexId().get() +
            ", vertexValue=" + vertex.getVertexValue() + ", destinationId=" +
            destVertexId + ", edgeValue=" + edgeValue);
        return true;
    }

    public long getPos() throws IOException {
        return m_recordsRead;
    }

    public void close() throws IOException {
    }

    public float getProgress() throws IOException {
        return m_recordsRead * 100.0f / m_totalRecords;
    }

    public LongWritable createVertexId() {
        return new LongWritable(-1);
    }

    public IntWritable createVertexValue() {
        return new IntWritable(-1);
    }

    public FloatWritable createEdgeValue() {
        return new FloatWritable(0.0f);
    }
}
