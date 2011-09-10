package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Global state of the graph.  Should be treated as a singleton (but is kept
 * as a regular bean to facilitate ease of unit testing)
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
@SuppressWarnings("rawtypes")
public class GraphState<I extends WritableComparable, V extends Writable,
        E extends Writable, M extends Writable> {
    /** Graph-wide superstep */
    private long superstep = 0;
    /** Graph-wide number of vertices */
    private long numVertices = -1;
    /** Graph-wide number of edges */
    private long numEdges = -1;
    /** Graph-wide map context */
    private Mapper.Context context = null;
    /** Graph-wide BSP Mapper for this Vertex */
    private GraphMapper<I, V, E, M> graphMapper = null;

    public long getSuperstep() {
        return superstep;
    }

    public GraphState<I, V, E, M> setSuperstep(long superstep) {
        this.superstep = superstep;
        return this;
    }

    public long getNumVertices() {
        return numVertices;
    }

    public GraphState<I, V, E, M> setNumVertices(long numVertices) {
        this.numVertices = numVertices;
        return this;
    }

    public long getNumEdges() {
        return numEdges;
    }

    public GraphState<I, V, E, M> setNumEdges(long numEdges) {
        this.numEdges = numEdges;
        return this;
    }

    public Mapper.Context getContext() {
        return context;
    }

    public GraphState<I, V , E ,M> setContext(Mapper.Context context) {
        this.context = context;
        return this;
    }

    public GraphMapper<I, V, E, M> getGraphMapper() {
        return graphMapper;
    }

    public GraphState<I, V, E, M> setGraphMapper(
            GraphMapper<I, V, E, M> graphMapper) {
        this.graphMapper = graphMapper;
        return this;
    }
}
