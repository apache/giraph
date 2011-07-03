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
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class GeneratedVertexReader implements
        VertexReader<LongWritable, IntWritable, FloatWritable> {
    /** Logger */
    private static final Logger LOG =
        Logger.getLogger(GeneratedVertexReader.class);
    /** Records read so far */
    private long recordsRead = 0;
    /** Total records to read (on this split alone) */
    private long totalRecords = 0;
    /** The input split from initialize(). */
    private BspInputSplit inputSplit = null;

    public static final String READER_VERTICES =
        "TestVertexReader.reader_vertices";
    public static final long DEFAULT_READER_VERTICES = 10;

    @Override
    public void initialize(
        InputSplit inputSplit, TaskAttemptContext context)
        throws IOException {
        Configuration configuration = context.getConfiguration();
            totalRecords = configuration.getLong(
                GeneratedVertexReader.READER_VERTICES,
                GeneratedVertexReader.DEFAULT_READER_VERTICES);
            this.inputSplit = (BspInputSplit) inputSplit;
    }

    @Override
    public boolean next(
            MutableVertex<LongWritable, IntWritable, FloatWritable, ?> vertex)
            throws IOException {
        if (totalRecords <= recordsRead) {
            return false;
        }
        vertex.setVertexId(new LongWritable(
            (inputSplit.getSplitIndex() * totalRecords) + recordsRead));
        vertex.setVertexValue(
            new IntWritable(((int) (vertex.getVertexId().get() * 10))));
        long destVertexId =
            (vertex.getVertexId().get() + 1) %
            (inputSplit.getNumSplits() * totalRecords);
        float edgeValue = (float) vertex.getVertexId().get() * 100;
        // Adds an edge to the neighbor vertex

        vertex.addEdge(new Edge<LongWritable, FloatWritable>(
            new LongWritable(destVertexId),
            new FloatWritable(edgeValue)));
        ++recordsRead;
        LOG.info("next: Return vertexId=" + vertex.getVertexId().get() +
            ", vertexValue=" + vertex.getVertexValue() + ", destinationId=" +
            destVertexId + ", edgeValue=" + edgeValue);
        return true;
    }

    @Override
    public long getPos() throws IOException {
        return recordsRead;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return recordsRead * 100.0f / totalRecords;
    }

    @Override
    public LongWritable createVertexId() {
        return new LongWritable(-1);
    }

    @Override
    public IntWritable createVertexValue() {
        return new IntWritable(-1);
    }

    @Override
    public FloatWritable createEdgeValue() {
        return new FloatWritable(0.0f);
    }
}
