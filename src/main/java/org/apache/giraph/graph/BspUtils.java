package org.apache.giraph.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Help to use the configuration to get the appropriate classes or
 * instantiate them.
 */
public class BspUtils {
    /**
     * Get the user's subclassed {@link VertexInputFormat}.
     *
     * @param conf Configuration to check
     * @return User's vertex input format class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable,
                   V extends Writable,
                   E extends Writable>
        Class<? extends VertexInputFormat<I, V, E>>
            getVertexInputFormatClass(Configuration conf) {
        return (Class<? extends VertexInputFormat<I, V, E>>)
                conf.getClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                              VertexInputFormat.class,
                              VertexInputFormat.class);
    }

    /**
     * Create a user vertex input format class
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex input format class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable> VertexInputFormat<I, V, E>
            createVertexInputFormat(Configuration conf) {
        Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
            getVertexInputFormatClass(conf);
        return ReflectionUtils.newInstance(vertexInputFormatClass, conf);
    }

    /**
     * Get the user's subclassed {@link VertexOutputFormat}.
     *
     * @param conf Configuration to check
     * @return User's vertex output format class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable,
                   V extends Writable,
                   E extends Writable>
        Class<? extends VertexOutputFormat<I, V, E>>
            getVertexOutputFormatClass(Configuration conf) {
        return (Class<? extends VertexOutputFormat<I, V, E>>)
                conf.getClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                              null,
                              VertexOutputFormat.class);
    }

    /**
     * Create a user vertex output format class
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex output format class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable> VertexOutputFormat<I, V, E>
            createVertexOutputFormat(Configuration conf) {
        Class<? extends VertexOutputFormat<I, V, E>> vertexOutputFormatClass =
            getVertexOutputFormatClass(conf);
        return ReflectionUtils.newInstance(vertexOutputFormatClass, conf);
    }

    /**
     * Get the user's subclassed vertex range balancer
     *
     * @param conf Configuration to check
     * @return User's vertex range balancer class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable,
                   V extends Writable,
                   E extends Writable,
                   M extends Writable>
        Class<? extends VertexRangeBalancer<I, V, E, M>>
            getBasicVertexRangeBalancerClass(Configuration conf) {
        return (Class<? extends VertexRangeBalancer<I, V, E, M>>)
                conf.getClass(GiraphJob.VERTEX_RANGE_BALANCER_CLASS,
                              StaticBalancer.class,
                              BasicVertexRangeBalancer.class);
    }

    /**
     * Create a user vertex range balancer class
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex input format class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable, M extends Writable> VertexRangeBalancer<I, V, E, M>
            createBasicVertexRangeBalancer(Configuration conf) {
        Class<? extends VertexRangeBalancer<I, V, E, M>>
            vertexRangeBalancerClass = getBasicVertexRangeBalancerClass(conf);
        return ReflectionUtils.newInstance(vertexRangeBalancerClass, conf);
    }

    /**
     * Get the user's subclassed VertexResolver.
     *
     * @param conf Configuration to check
     * @return User's vertex resolver class
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <I extends WritableComparable,
                   V extends Writable,
                   E extends Writable,
                   M extends Writable>
            Class<? extends BspResolver<I, V, E, M>>
            getVertexResolverClass(Configuration conf) {
        return (Class<? extends BspResolver<I, V, E, M>>)
                conf.getClass(GiraphJob.VERTEX_RESOLVER_CLASS,
                              BspResolver.class,
                              BspResolver.class);
    }

    /**
     * Create a user vertex revolver
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex resolver
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable, M extends Writable> BspResolver<I, V, E, M>
            createVertexResolver(Configuration conf) {
        Class<? extends BspResolver<I, V, E, M>> vertexResolverClass =
            getVertexResolverClass(conf);
        return ReflectionUtils.newInstance(vertexResolverClass, conf);
    }

    /**
     * Get the user's subclassed Vertex.
     *
     * @param conf Configuration to check
     * @return User's vertex class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable,
                   V extends Writable,
                   E extends Writable,
                   M extends Writable>
            Class<? extends Vertex<I, V, E, M>>
            getVertexClass(Configuration conf) {
        return (Class<? extends Vertex<I, V, E, M>>)
                conf.getClass(GiraphJob.VERTEX_CLASS,
                              Vertex.class,
                              Vertex.class);
    }

    /**
     * Create a user vertex
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable, M extends Writable> Vertex<I, V, E, M>
            createVertex(Configuration conf) {
        Class<? extends Vertex<I, V, E, M>> vertexClass =
            getVertexClass(conf);
        return ReflectionUtils.newInstance(vertexClass, conf);
    }

    /**
     * Get the user's subclassed vertex index class.
     *
     * @param conf Configuration to check
     * @return User's vertex index class
     */
    @SuppressWarnings("unchecked")
    public static <I extends Writable> Class<I>
            getVertexIndexClass(Configuration conf) {
        return (Class<I>) conf.getClass(GiraphJob.VERTEX_INDEX_CLASS,
                                        WritableComparable.class);
    }

    /**
     * Create a user vertex index
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex index
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable>
            I createVertexIndex(Configuration conf) {
        Class<I> vertexClass = getVertexIndexClass(conf);
        try {
            return vertexClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                "createVertexIndex: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                "createVertexIndex: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed vertex value class.
     *
     * @param conf Configuration to check
     * @return User's vertex value class
     */
    @SuppressWarnings("unchecked")
    public static <V extends Writable> Class<V>
            getVertexValueClass(Configuration conf) {
        return (Class<V>) conf.getClass(GiraphJob.VERTEX_VALUE_CLASS,
                                        Writable.class);
    }

    /**
     * Create a user vertex value
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex value
     */
    public static <V extends Writable> V
            createVertexValue(Configuration conf) {
        Class<V> vertexValueClass = getVertexValueClass(conf);
        try {
            return vertexValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                "createVertexValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                "createVertexValue: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed edge value class.
     *
     * @param conf Configuration to check
     * @return User's vertex edge value class
     */
    @SuppressWarnings("unchecked")
    public static <E extends Writable> Class<E>
            getEdgeValueClass(Configuration conf){
        return (Class<E>) conf.getClass(GiraphJob.EDGE_VALUE_CLASS,
                                        Writable.class);
    }

    /**
     * Create a user edge value
     *
     * @param conf Configuration to check
     * @return Instantiated user edge value
     */
    public static <E extends Writable> E
            createEdgeValue(Configuration conf) {
        Class<E> edgeValueClass = getEdgeValueClass(conf);
        try {
            return edgeValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                "createEdgeValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                "createEdgeValue: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed vertex message value class.
     *
     * @param conf Configuration to check
     * @return User's vertex message value class
     */
    @SuppressWarnings("unchecked")
    public static <M extends Writable> Class<M>
            getMessageValueClass(Configuration conf) {
        return (Class<M>) conf.getClass(GiraphJob.MESSAGE_VALUE_CLASS,
                                        Writable.class);
    }

    /**
     * Create a user vertex message value
     *
     * @param conf Configuration to check
     * @return Instantiated user vertex message value
     */
    public static <M extends Writable> M
            createMessageValue(Configuration conf) {
        Class<M> MessageValueClass = getMessageValueClass(conf);
        try {
            return MessageValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                "createMessageValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                "createMessageValue: Illegally accessed", e);
        }
    }
}
