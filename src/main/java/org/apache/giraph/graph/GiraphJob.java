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

import java.io.IOException;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, our job should not have any reduce tasks.
 */
public class GiraphJob extends Job {
    /** Vertex class - required */
    public static final String VERTEX_CLASS = "giraph.vertexClass";
    /** VertexInputFormat class - required */
    public static final String VERTEX_INPUT_FORMAT_CLASS =
        "giraph.vertexInputFormatClass";

    /** VertexOutputFormat class - optional */
    public static final String VERTEX_OUTPUT_FORMAT_CLASS =
        "giraph.vertexOutputFormatClass";
    /** Vertex combiner class - optional */
    public static final String VERTEX_COMBINER_CLASS =
        "giraph.combinerClass";
    /** Vertex range balancer class - optional */
    public static final String VERTEX_RANGE_BALANCER_CLASS =
        "giraph.vertexRangeBalancerClass";
    /** Vertex resolver class - optional */
    public static final String VERTEX_RESOLVER_CLASS =
        "giraph.vertexResolverClass";

    /** Vertex index class */
    public static final String VERTEX_INDEX_CLASS = "giraph.vertexIndexClass";
    /** Vertex value class */
    public static final String VERTEX_VALUE_CLASS = "giraph.vertexValueClass";
    /** Edge value class */
    public static final String EDGE_VALUE_CLASS = "giraph.edgeValueClass";
    /** Message value class */
    public static final String MESSAGE_VALUE_CLASS = "giraph.messageValueClass";

    /**
     * Minimum number of simultaneous workers before this job can run (int)
     */
    public static final String MIN_WORKERS = "giraph.minWorkers";
    /**
     * Maximum number of simultaneous worker tasks started by this job (int).
     */
    public static final String MAX_WORKERS = "giraph.maxWorkers";

    /**
     * Separate the workers and the master tasks.  This is required
     * to support dynamic recovery. (boolean)
     */
    public static final String SPLIT_MASTER_WORKER =
        "giraph.SplitMasterWorker";
    /**
     * Default on whether to separate the workers and the master tasks.
     * Needs to be "true" to support dynamic recovery.
     */
    public static final boolean SPLIT_MASTER_WORKER_DEFAULT = true;

    /**
     * Minimum percent of the maximum number of workers that have responded
     * in order to continue progressing. (float)
     */
    public static final String MIN_PERCENT_RESPONDED =
        "giraph.minPercentResponded";
    /** Default 100% response rate for workers */
    public static final float MIN_PERCENT_RESPONDED_DEFAULT = 100.0f;

    /** Polling timeout to check on the number of responded tasks (int) */
    public static final String POLL_MSECS = "giraph.pollMsecs";
    /** Default poll msecs (30 seconds) */
    public static final int POLL_MSECS_DEFAULT = 30*1000;

    /**
     *  ZooKeeper comma-separated list (if not set,
     *  will start up ZooKeeper locally)
     */
    public static final String ZOOKEEPER_LIST = "giraph.zkList";

    /** ZooKeeper session millisecond timeout */
    public static final String ZOOKEEPER_SESSION_TIMEOUT =
        "giraph.zkSessionMsecTimeout";
    /** Default Zookeeper session millisecond timeout */
    public static final int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 60*1000;

    /** Polling interval to check for the final ZooKeeper server data */
    public static final String ZOOKEEPER_SERVERLIST_POLL_MSECS =
        "giraph.zkServerlistPollMsecs";
    /** Default polling interval to check for the final ZooKeeper server data */
    public static final int ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT =
        3*1000;

    /** Number of nodes (not tasks) to run Zookeeper on */
    public static final String ZOOKEEPER_SERVER_COUNT =
        "giraph.zkServerCount";
    /** Default number of nodes to run Zookeeper on */
    public static final int ZOOKEEPER_SERVER_COUNT_DEFAULT = 1;

    /** ZooKeeper port to use */
    public static final String ZOOKEEPER_SERVER_PORT =
        "giraph.zkServerPort";
    /** Default ZooKeeper port to use */
    public static final int ZOOKEEPER_SERVER_PORT_DEFAULT = 22181;

    /** Location of the ZooKeeper jar - Used internally, not meant for users */
    public static final String ZOOKEEPER_JAR = "giraph.zkJar";

    /** Local ZooKeeper directory to use */
    public static final String ZOOKEEPER_DIR = "giraph.zkDir";

    /** Initial port to start using for the RPC communication */
    public static final String RPC_INITIAL_PORT = "giraph.rpcInitialPort";
    /** Default port to start using for the RPC communication */
    public static final int RPC_INITIAL_PORT_DEFAULT = 30000;

    /** Maximum number of RPC handlers */
    public static final String RPC_NUM_HANDLERS = "giraph.rpcNumHandlers";
    /** Default maximum number of RPC handlers */
    public static final int RPC_NUM_HANDLERS_DEFAULT = 100;

    /** Maximum number of messages per peer before flush */
    public static final String MSG_SIZE = "giraph.msgSize";
    /** Default maximum number of messages per peer before flush */
    public static final int MSG_SIZE_DEFAULT = 1000;

    /** Number of poll attempts prior to failing the job (int) */
    public static final String POLL_ATTEMPTS = "giraph.pollAttempts";
    /** Default poll attempts */
    public static final int POLL_ATTEMPTS_DEFAULT = 10;

    /** Number of minimum vertices in each vertex range */
    public static final String MIN_VERTICES_PER_RANGE =
        "giraph.minVerticesPerRange";
    /** Default number of minimum vertices in each vertex range */
    public static final long MIN_VERTICES_PER_RANGE_DEFAULT = 3;

    /** Minimum stragglers of the superstep before printing them out */
    public static final String PARTITION_LONG_TAIL_MIN_PRINT =
        "giraph.partitionLongTailMinPrint";
    /** Only print stragglers with one as a default */
    public static final int PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT = 1;

    /** Use superstep counters? (boolean) */
    public static final String USE_SUPERSTEP_COUNTERS =
        "giraph.useSuperstepCounters";
    /** Default is to use the superstep counters */
    public static final boolean USE_SUPERSTEP_COUNTERS_DEFAULT = true;

    /**
     * Set the multiplicative factor of how many partitions to create from
     * a single InputSplit based on the number of total InputSplits.  For
     * example, if there are 10 total InputSplits and this is set to 0.5, then
     * you will get 0.5 * 10 = 5 partitions for every InputSplit (given that the
     * minimum size is met).
     */
    public static final String TOTAL_INPUT_SPLIT_MULTIPLIER =
        "giraph.totalInputSplitMultiplier";
    /** Default total input split multiplier */
    public static final float TOTAL_INPUT_SPLIT_MULTIPLIER_DEFAULT = 0.5f;

    /** Java opts passed to ZooKeeper startup */
    public static final String ZOOKEEPER_JAVA_OPTS =
        "giraph.zkJavaOpts";
    /** Default java opts passed to ZooKeeper startup */
    public static final String ZOOKEEPER_JAVA_OPTS_DEFAULT =
        "-Xmx256m -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC " +
        "-XX:CMSInitiatingOccupancyFraction=70 -XX:MaxGCPauseMillis=100";

    /**
     *  How often to checkpoint (i.e. 0, means no checkpoint,
     *  1 means every superstep, 2 is every two supersteps, etc.).
     */
    public static final String CHECKPOINT_FREQUENCY =
        "giraph.checkpointFrequency";

    /** Default checkpointing frequency of every 2 supersteps. */
    public static final int CHECKPOINT_FREQUENCY_DEFAULT = 2;

    /**
     * Delete checkpoints after a successful job run?
     */
    public static final String CLEANUP_CHECKPOINTS_AFTER_SUCCESS =
        "giraph.cleanupCheckpointsAfterSuccess";
    /** Default is to clean up the checkponts after a successful job */
    public static final boolean CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT =
        true;

    /**
     * An application can be restarted manually by selecting a superstep.  The
     * corresponding checkpoint must exist for this to work.  The user should
     * set a long value.  Default is start from scratch.
     */
    public static final String RESTART_SUPERSTEP = "giraph.restartSuperstep";

    /**
     * If ZOOKEEPER_LIST is not set, then use this directory to manage
     * ZooKeeper
     */
    public static final String ZOOKEEPER_MANAGER_DIRECTORY =
        "giraph.zkManagerDirectory";
    /**
     * Default ZooKeeper manager directory (where determining the servers in
     * HDFS files will go).  Final directory path will also have job number
     * for uniqueness.
     */
    public static final String ZOOKEEPER_MANAGER_DIR_DEFAULT =
        "_bsp/_defaultZkManagerDir";

    /** This directory has/stores the available checkpoint files in HDFS. */
    public static final String CHECKPOINT_DIRECTORY =
        "giraph.checkpointDirectory";
    /**
     * Default checkpoint directory (where checkpoing files go in HDFS).  Final
     * directory path will also have the job number for uniqueness
     */
    public static final String CHECKPOINT_DIRECTORY_DEFAULT =
        "_bsp/_checkpoints/";

    /** Keep the zookeeper output for debugging? Default is to remove it. */
    public static final String KEEP_ZOOKEEPER_DATA =
        "giraph.keepZooKeeperData";
    /** Default is to remove ZooKeeper data. */
    public static final Boolean KEEP_ZOOKEEPER_DATA_DEFAULT = false;

    /** Default ZooKeeper tick time. */
    public static final int DEFAULT_ZOOKEEPER_TICK_TIME = 2000;
    /** Default ZooKeeper init limit (in ticks). */
    public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;
    /** Default ZooKeeper sync limit (in ticks). */
    public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;
    /** Default ZooKeeper snap count. */
    public static final int DEFAULT_ZOOKEEPER_SNAP_COUNT = 50000;
    /** Default ZooKeeper maximum client connections. */
    public static final int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 10000;
    /** Default ZooKeeper minimum session timeout (in msecs). */
    public static final int DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT = 10000;
    /** Default ZooKeeper maximum session timeout (in msecs). */
    public static final int DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT = 100000;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GiraphJob.class);

    /**
     * Constructor that will instantiate the configuration
     *
     * @param jobName User-defined job name
     * @throws IOException
     */
    public GiraphJob(String jobName) throws IOException {
        super(new Configuration(), jobName);
    }

    /**
     * Constructor.
     *
     * @param conf User-defined configuration
     * @param jobName User-defined job name
     * @throws IOException
     */
    public GiraphJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
    }

    /**
     * Make sure the configuration is set properly by the user prior to
     * submitting the job.
     */
    private void checkConfiguration() {
        if (conf.getInt(MAX_WORKERS, -1) < 0) {
            throw new RuntimeException("No valid " + MAX_WORKERS);
        }
        if (conf.getFloat(MIN_PERCENT_RESPONDED,
                          MIN_PERCENT_RESPONDED_DEFAULT) <= 0.0f ||
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) > 100.0f) {
            throw new IllegalArgumentException(
                "Invalid " +
                conf.getFloat(MIN_PERCENT_RESPONDED,
                              MIN_PERCENT_RESPONDED_DEFAULT) + " for " +
                MIN_PERCENT_RESPONDED);
        }
        if (conf.getInt(MIN_WORKERS, -1) < 0) {
            throw new IllegalArgumentException("No valid " + MIN_WORKERS);
        }
        if (BspUtils.getVertexClass(getConfiguration()) == null) {
            throw new IllegalArgumentException("GiraphJob: Null VERTEX_CLASS");
        }
        if (BspUtils.getVertexInputFormatClass(getConfiguration()) == null) {
            throw new IllegalArgumentException(
                "GiraphJob: Null VERTEX_INPUT_FORMAT_CLASS");
        }
        if (BspUtils.getVertexResolverClass(getConfiguration()) == null) {
            setVertexResolverClass(VertexResolver.class);
            if (LOG.isInfoEnabled()) {
                LOG.info("GiraphJob: No class found for " +
                         VERTEX_RESOLVER_CLASS + ", defaulting to " +
                         VertexResolver.class.getCanonicalName());
            }
        }
    }

    /**
     * Set the vertex class (required)
     *
     * @param vertexClass Runs vertex computation
     */
    final public void setVertexClass(Class<?> vertexClass) {
        getConfiguration().setClass(VERTEX_CLASS, vertexClass, Vertex.class);
    }

    /**
     * Set the vertex input format class (required)
     *
     * @param vertexInputFormatClass Determines how graph is input
     */
    final public void setVertexInputFormatClass(
            Class<?> vertexInputFormatClass) {
        getConfiguration().setClass(VERTEX_INPUT_FORMAT_CLASS,
                                    vertexInputFormatClass,
                                    VertexInputFormat.class);
    }

    /**
     * Set the vertex output format class (optional)
     *
     * @param vertexOutputFormatClass Determines how graph is output
     */
    final public void setVertexOutputFormatClass(
            Class<?> vertexOutputFormatClass) {
        getConfiguration().setClass(VERTEX_OUTPUT_FORMAT_CLASS,
                                    vertexOutputFormatClass,
                                    VertexOutputFormat.class);
    }

    /**
     * Set the vertex combiner class (optional)
     *
     * @param vertexCombinerClass Determines how vertex messages are combined
     */
    final public void setVertexCombinerClass(Class<?> vertexCombinerClass) {
        getConfiguration().setClass(VERTEX_COMBINER_CLASS,
                                    vertexCombinerClass,
                                    VertexCombiner.class);
    }

    /**
     * Set the vertex range balancer class (optional)
     *
     * @param vertexRangeBalancerClass Determines how vertex
     *        ranges are balanced prior to each superstep
     */
    final public void setVertexRangeBalancerClass(
            Class<?> vertexRangeBalancerClass) {
        getConfiguration().setClass(VERTEX_RANGE_BALANCER_CLASS,
                                    vertexRangeBalancerClass,
                                    VertexRangeBalancer.class);
    }

    /**
     * Set the vertex resolver class (optional)
     *
     * @param vertexResolverClass Determines how vertex mutations are resolved
     */
    final public void setVertexResolverClass(Class<?> vertexResolverClass) {
        getConfiguration().setClass(VERTEX_RESOLVER_CLASS,
                                    vertexResolverClass,
                                    VertexResolver.class);
    }

    /**
     * Set worker configuration for determining what is required for
     * a superstep.
     *
     * @param minWorkers Minimum workers to do a superstep
     * @param maxWorkers Maximum workers to do a superstep
     *        (max map tasks in job)
     * @param minPercentResponded 0 - 100 % of the workers required to
     *        have responded before continuing the superstep
     */
    final public void setWorkerConfiguration(int minWorkers,
                                             int maxWorkers,
                                             float minPercentResponded) {
        conf.setInt(MIN_WORKERS, minWorkers);
        conf.setInt(MAX_WORKERS, maxWorkers);
        conf.setFloat(MIN_PERCENT_RESPONDED, minPercentResponded);
    }

    /**
     * Utilize an existing ZooKeeper service.  If this is not set, ZooKeeper
     * will be dynamically started by Giraph for this job.
     *
     * @param serverList Comma separated list of servers and ports
     *        (i.e. zk1:2221,zk2:2221)
     */
    final public void setZooKeeperConfiguration(String serverList) {
        conf.set(ZOOKEEPER_LIST, serverList);
    }

    /**
     * Check if the configuration is local.  If it is local, do additional
     * checks due to the restrictions of LocalJobRunner.
     *
     * @param conf Configuration
     */
    private static void checkLocalJobRunnerConfiguration(
            Configuration conf) {
        String jobTracker = conf.get("mapred.job.tracker", null);
        if (!jobTracker.equals("local")) {
            // Nothing to check
            return;
        }

        int maxWorkers = conf.getInt(MAX_WORKERS, -1);
        if (maxWorkers != 1) {
            throw new IllegalArgumentException(
                "checkLocalJobRunnerConfiguration: When using " +
                "LocalJobRunner, must have only one worker since " +
                "only 1 task at a time!");
        }
        if (conf.getBoolean(SPLIT_MASTER_WORKER,
                            SPLIT_MASTER_WORKER_DEFAULT)) {
            throw new IllegalArgumentException(
                "checkLocalJobRunnerConfiguration: When using " +
                "LocalJobRunner, you cannot run in split master / worker " +
                "mode since there is only 1 task at a time!");
        }
    }

    /**
     * Check whether a specified int conf value is set and if not, set it.
     *
     * @param param Conf value to check
     * @param defaultValue Assign to value if not set
     */
    private void setIntConfIfDefault(String param, int defaultValue) {
        if (conf.getInt(param, Integer.MIN_VALUE) == Integer.MIN_VALUE) {
            conf.setInt(param, defaultValue);
        }
    }

    /**
     * Runs the actual graph application through Hadoop Map-Reduce.
     *
     * @param verbose If true, provide verbose output, false otherwise
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    final public boolean run(boolean verbose)
            throws IOException, InterruptedException, ClassNotFoundException {
        checkConfiguration();
        checkLocalJobRunnerConfiguration(conf);
        setNumReduceTasks(0);
        // Most users won't hit this hopefully and can set it higher if desired
        setIntConfIfDefault("mapreduce.job.counters.limit", 512);

        // Capacity scheduler-specific settings.  These should be enough for
        // a reasonable Giraph job
        setIntConfIfDefault("mapred.job.map.memory.mb", 1024);
        setIntConfIfDefault("mapred.job.reduce.memory.mb", 1024);

        if (getJar() == null) {
            setJarByClass(GiraphJob.class);
        }
        // Should work in MAPREDUCE-1938 to let the user jars/classes
        // get loaded first
        conf.setBoolean("mapreduce.user.classpath.first", true);
        setMapperClass(GraphMapper.class);
        setInputFormatClass(BspInputFormat.class);
        setOutputFormatClass(BspOutputFormat.class);
        return waitForCompletion(verbose);
    }
}
