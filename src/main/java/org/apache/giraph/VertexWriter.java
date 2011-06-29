package org.apache.giraph;

import java.io.IOException;
import java.util.SortedMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Implement to output the graph after the computation
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
@SuppressWarnings("rawtypes")
public interface VertexWriter<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable> {

    /**
     * Writes the argument vertex and associated data
     *
     * @param context output context
     * @param vertexId vertex id that is written out
     * @param vertexValue vertex value that is written out
     * @param destEdgeIt iterator over vertex edges written out
     */
    <KEYOUT,VALUEOUT> void write(
        TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context,
            I vertexId, V vertexValue, SortedMap<I, Edge<I, E>> outEdgeMap)
        throws IOException, InterruptedException;

    /**
     * Close this {@link VertexWriter} to future operations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    void close(TaskAttemptContext context)
        throws IOException, InterruptedException;
}
