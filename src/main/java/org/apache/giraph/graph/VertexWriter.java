package org.apache.giraph.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implement to output a vertex range of the graph after the computation
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface VertexWriter<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable> {
    /**
     * Use the context to setup writing the vertices.
     * Guaranteed to be called prior to any other function.
     *
     * @param context
     * @throws IOException
     */
    void initialize(TaskAttemptContext context) throws IOException;

    /**
     * Writes the next vertex and associated data
     *
     * @param vertex set the properties of this vertex
     * @throws IOException
     */
    void writeVertex(BasicVertex<I, V, E, ?> vertex) throws IOException;

    /**
     * Close this {@link VertexWriter} to future operations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    void close(TaskAttemptContext context)
        throws IOException;
}
