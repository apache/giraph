package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, our job should not have any reduce tasks.
 * 
 * @author aching
 * @param <E>
 * @param <M>
 * @param <V>
 */
public class BspJob<V, E, M> extends Job {
	/** Minimum number of simultaneous processes before this job can run (int) */
	public static final String BSP_MIN_PROCESSES = "bsp.minProcs";
	/** Initial number of simultaneous tasks started by this job (int) */
	public static final String BSP_INITIAL_PROCESSES = "bsp.maxProcs";
	/** Minimum percent of initial processes that have responded (float) */
	public static final String BSP_MIN_PERCENT_RESPONDED = 
		"bsp.minPercentResponded";
	/** Polling timeout to check on the number of responded tasks (int) */
	public static final String BSP_POLL_MSECS = "bsp.pollMsecs";
	/** ZooKeeper list (empty for start up ZooKeeper locally) */
	public static final String BSP_ZOOKEEPER_LIST = "bsp.zkList";
	/** ZooKeeper session millisecond timeout */
	public static final String BSP_ZOOKEEPER_SESSION_TIMEOUT = 
		"bsp.zkSessionMsecTimeout";
	/** Polling interval to check for the final ZooKeeper server data */
	public static final String BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS = 
		"bsp.zkServerlistPollMsecs";
	/** Number of nodes to run Zookeeper on */
	public static final String BSP_ZOOKEEPER_SERVER_COUNT =
		"bsp.zkServerCount";
	/** ZooKeeper port to use */
	public static final String BSP_ZOOKEEPER_SERVER_PORT =
		"bsp.zkServerPort";
	/** Location of the ZooKeeper jar - Used internally, not meant for users */
	public static final String BSP_ZOOKEEPER_JAR = "bsp.zkJar";
	/** Local ZooKeeper directory to use */
	public static final String BSP_ZOOKEEPER_DIR = "bsp.zkDir";
	/** Initial port to start using for the RPC communication */
	public static final String BSP_RPC_INITIAL_PORT = "bsp.rpcInitialPort";
	/** Default port to start using for the RPC communication */
	public static final int BSP_RPC_DEFAULT_PORT = 61000;
	/** Maximum number of messages per peer before flush */
	public static final String BSP_MSG_SIZE = "bsp.msgSize";
	/** Default maximum number of messages per peer before flush */
	public static int BSP_MSG_DEFAULT_SIZE = 1000;

	/** 
	 * If BSP_ZOOKEEPER_LIST is not set, then use this directory to manage 
	 * ZooKeeper 
	 */
	public static final String BSP_ZOOKEEPER_MANAGER_DIRECTORY = 
		"bsp.zkManagerDirectory";
	/** Default poll msecs (30 seconds) */
	public static final int DEFAULT_BSP_POLL_MSECS = 30*1000;
	/** Number of poll attempts prior to failing the job (int) */
	public static final String BSP_POLL_ATTEMPTS = "bsp.pollAttempts";
	/** Default poll attempts */
	public static final int DEFAULT_BSP_POLL_ATTEMPTS = 5;
	/** Default Zookeeper session millisecond timeout */
	public static final int DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT = 60*1000;
	/** Default polling interval to check for the final ZooKeeper server data */
	public static final int DEFAULT_BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS = 
		3*1000;
	/** Default number of nodes to run Zookeeper on */
	public static final int DEFAULT_BSP_ZOOKEEPER_SERVER_COUNT = 1;
	/** Default ZooKeeper port to use */
	public static final int DEFAULT_BSP_ZOOKEEPER_SERVER_PORT = 22181;
	/** 
	 * Default local ZooKeeper directory to use (where ZooKeeper server 
	 * files will go) 
	 */
	public static final String DEFAULT_BSP_ZOOKEEPER_DIR = "/tmp/bspZooKeeper";
	/** 
	 * Default ZooKeeper manager directory (where determining the servers in 
	 * HDFS files will go) 
	 */
	public static final String DEFAULT_ZOOKEEPER_MANAGER_DIRECTORY =
		"/tmp/_defaultZkManagerDir";
	
	/**
	 *  Constructor.
	 * @param conf user-defined configuration
	 * @param jobName user-defined job name
	 * @throws IOException
	 */
	public BspJob(
			Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
		if (conf.getInt(BSP_INITIAL_PROCESSES, -1) < 0) {
			throw new IOException("No valid " + BSP_INITIAL_PROCESSES);
		}
		if (conf.getFloat(BSP_MIN_PERCENT_RESPONDED, 0.0f) <= 0) {
			throw new IOException("No valid " + BSP_MIN_PERCENT_RESPONDED);
		}
		if (conf.getInt(BSP_MIN_PROCESSES, -1) < 0) {
			throw new IOException("No valid " + BSP_MIN_PROCESSES);
		}
	}
	
	/**
	 * The mapper that will execute the BSP tasks.  Since this mapper will
	 * not be passing data by key-value pairs through the MR framework, the 
	 * types are irrelevant.
	 * 
	 * @author aching
	 * @param <V>
	 * @param <V>
	 */
	public static class BspMapper<I extends Writable, V, E, M extends Writable>
		extends Mapper<Object, Object, Object, Object> {
		/** Logger */
		private static final Logger LOG = Logger.getLogger(BspMapper.class);
		/** Data structure for managing vertices */
		List<HadoopVertex<I, V, E, M>> m_vertexList = 
			new ArrayList<HadoopVertex<I, V, E, M>>();
		/** Coordination */
		CentralizedService<I> m_service;
		/** Communication */
		private RPCCommunications<I, M> m_commService;
		/** The map should be run exactly once, or else there is a problem. */
		boolean m_mapAlreadyRun = false;
		/** Manages the ZooKeeper server if necessary */
		ZooKeeperManager m_manager;
		/** Configuration */
		Configuration m_conf = null;
		/** Already complete? */
		boolean m_done = false;
		
		/**
		 * Load the vertices from the user-defined VertexReader into our 
		 * vertexArray.  As per the VertexInputFormat, determine the partitions
		 * based on the split.
		 * @throws IllegalAccessException 
		 * @throws InstantiationException 
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void loadVertices(Context context) 
		    throws InstantiationException, IllegalAccessException, IOException {			
			InputSplit myInputSplit = m_service.getInputSplit();
			@SuppressWarnings("unchecked")
			Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass = 
				(Class<? extends VertexInputFormat<I, V, E>>) 
					m_conf.getClass("bsp.vertexInputFormatClass", 
							        VertexInputFormat.class);
			@SuppressWarnings("rawtypes")
			Class<? extends HadoopVertex> vertexClass = 
				m_conf.getClass("bsp.vertexClass", 
								HadoopVertex.class, 
								HadoopVertex.class);
			VertexInputFormat<I, V, E> vertexInputFormat = 
				vertexInputFormatClass.newInstance();
			VertexReader<I, V, E> vertexReader = 
				vertexInputFormat.createVertexReader(myInputSplit, context);
			vertexReader.initialize(myInputSplit, context);

			Map<I, E> destVertexIdEdgeValueMap = new TreeMap<I, E>();
	        I vertexId = vertexReader.createVertexId();
	        V vertexValue = vertexReader.createVertexValue();
			I vertexIdMax = vertexReader.createVertexId();
			while (vertexReader.next(
			       vertexId, vertexValue, destVertexIdEdgeValueMap)) {
				@SuppressWarnings("unchecked")
				HadoopVertex<I, V, E, M> vertex = 
					vertexClass.newInstance();
				vertex.setBspMapper(this);
				vertex.setId(vertexId);
				vertex.setVertexValue(vertexValue);
				for (Map.Entry<I, E> destVertexIdEdgeValue : 
				     destVertexIdEdgeValueMap.entrySet()) {
					vertex.addEdge(destVertexIdEdgeValue.getKey(),
					               destVertexIdEdgeValue.getValue());
				}
				m_vertexList.add(vertex);
				destVertexIdEdgeValueMap.clear();
				@SuppressWarnings("unchecked")
				Comparable<I> comparable =
						(Comparable<I>) vertexId;
				if (comparable.compareTo(vertexIdMax) > 0) {
					vertexIdMax = vertexId;
				}
	      vertexId = vertexReader.createVertexId();
	      vertexValue = vertexReader.createVertexValue();
			}
      vertexReader.close();
			m_service.setPartitionMax(vertexIdMax);
		}
			
		/**
		 * Save the vertices using the user-defined OutputFormat from our 
		 * vertexArray.
		 * based on the split.
		 * @throws IllegalAccessException 
		 * @throws InstantiationException 
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void saveVertices(Context context)
		    throws InstantiationException, IllegalAccessException,
                   IOException, InterruptedException {			
		    if (m_conf.get("bsp.vertexWriterClass") == null) {
		        LOG.warn("bsp.vertexWriterClass not specified" + 
                          " -- there will be no saved output");
		        return;
		    }

			@SuppressWarnings("unchecked")
			Class<? extends VertexWriter<I, V, E>> vertexWriterClass = 
				  (Class<? extends VertexWriter<I, V, E>>) 
					m_conf.getClass("bsp.vertexWriterClass", 
							            VertexWriter.class);
			VertexWriter<I, V, E> vertexWriter = 
				  vertexWriterClass.newInstance();
      for (HadoopVertex<I, V, E, M> vertex : m_vertexList) {
        vertexWriter.write(context, vertex.id(),
                           vertex.getVertexValue(),
                           vertex.getOutEdgeIterator());
      }
      vertexWriter.close(context);
    }
			
		/**
		 * Passes message on to communication service.
		 * 
		 * @param indx
		 * @param msg
		 */
		public void sendMsg(I indx, M msg) {
			m_commService.sendMessage(indx, msg);
		}

		@Override
		public void setup(Context context) 
			throws IOException, InterruptedException {
			m_conf = context.getConfiguration();
			/*
			 * Do some initial setup (possibly starting up a Zookeeper service), 
			 * but mainly decide whether to load data 
			 * from a checkpoint or from the InputFormat.
			 */
			String jarFile = context.getJar();
			String trimmedJarFile = jarFile.replaceFirst("file:", "");
			LOG.info("setup: jar file @ " + jarFile + 
					 ", using " + trimmedJarFile);
			m_conf.set(BSP_ZOOKEEPER_JAR, trimmedJarFile);
			String serverPortList = 
				m_conf.get(BspJob.BSP_ZOOKEEPER_LIST, "");
			if (serverPortList == "") {
				m_manager = new ZooKeeperManager(m_conf);
				m_manager.setup();
				if (m_manager.computationDone()) {
				    m_done = false;
				    return;
				}
				m_manager.onlineZooKeeperServers();
				serverPortList = m_manager.getZooKeeperServerPortString();
			}
			int sessionMsecTimeout = 
				m_conf.getInt(
					BspJob.BSP_ZOOKEEPER_SESSION_TIMEOUT,
					BspJob.DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT);
				try {
					LOG.info("Starting up BspService...");
					m_service = new BspService<I>(
						serverPortList, sessionMsecTimeout, m_conf);
					LOG.info("Registering health of this process...");
					m_service.setup();
					LOG.info("Loading the vertices...");
					loadVertices(context);
				} catch (Exception e) {
					LOG.error(e.getMessage());
					if (m_manager != null ) {
					    m_manager.offlineZooKeeperServers(0);
					}
					throw new RuntimeException(e);
				}
		}
		
		@Override
		public void map(Object key, Object value, Context context)
			throws IOException, InterruptedException {
			/*
			 * map() simply loads the data from the InputFormat to this mapper.
			 * If a checkpoint exists, this function should not have been
			 * called. 
			 * 
			 * 1) Load the data of all vertices on this mapper.
			 * 2) Run checkpoint per frequency policy.
			 * 3) For every vertex on this mapper, run the compute() function
			 * 4) Wait until all messaging is done.
			 * 5) Check if all vertices are done.  If not goto 2).
			 * 6) Dump output.
			 */
		    if (m_done == true) {
		        return;
		    }
		    
			if (m_mapAlreadyRun) {
				throw new RuntimeException("In BSP, map should have only been" +
										   " run exactly once, (already run)");
			}
			m_mapAlreadyRun = true;
			long verticesDone = 0;
			try {
			    while (!m_service.barrier(verticesDone, m_vertexList.size())) {
              long superStep = m_service.getSuperStep();
			        LOG.info("map: superstep = " + superStep);
			        LOG.info("totalMem=" + Runtime.getRuntime().totalMemory() +
                             " maxMem=" + Runtime.getRuntime().maxMemory() +
                             " freeMem=" + Runtime.getRuntime().freeMemory());
			        if (m_service.getSuperStep() == 0) {
			            LOG.info("Starting communication service...");
			            m_commService = new RPCCommunications<I, M>(
							m_conf, m_service);
			        }
              context.progress();
			        verticesDone = 0;
			        HadoopVertex.setSuperstep(superStep);
			        HadoopVertex.setNumVertices(m_service.getTotalVertices());
			        for (HadoopVertex<I, V, E, M> vertex : m_vertexList) {
			            if (!vertex.isHalted()) { 
			                Iterator<M> vertexMsgIt = 
			                    m_commService.getVertexMessageIterator(vertex.id());
                            context.progress();
			                vertex.compute(vertexMsgIt);
			            }
			            if (vertex.isHalted()) {
			                ++verticesDone;
			            }
			        }
              context.progress();
			        LOG.info("totalMem=" + Runtime.getRuntime().totalMemory() +
                             " maxMem=" + Runtime.getRuntime().maxMemory() +
                             " freeMem=" + Runtime.getRuntime().freeMemory());
			        m_commService.flush();
			        LOG.info("All " + m_vertexList.size() + 
						           " vertices finished superstep " + 
						           m_service.getSuperStep() + " (" + verticesDone + 
						           " of " + m_vertexList.size() + " vertices done)");
			    } 
			    LOG.info("BSP application done (global vertices marked done)");

          context.progress();
          saveVertices(context);
      } catch (InstantiationException e) {
          LOG.error(e.getMessage());
      } catch (IllegalAccessException e) {
          LOG.error(e.getMessage());
      } catch (Exception e) {
          LOG.error(e.getMessage());
			    if (m_manager != null) {
			        m_manager.offlineZooKeeperServers(0);
			    }
				  throw new RuntimeException(e);
      }
			
		}
		
		@Override
		public void cleanup(Context context) 
			throws IOException, InterruptedException {
			LOG.info("cleanup: Client done.");
			if (m_done) {
			    return;
			}
			
	        m_commService.close();
	        int totalPartitions = m_service.getPartitionSet().size();
			m_service.cleanup();
			if (m_manager != null) {
				m_manager.offlineZooKeeperServers(totalPartitions);
			}
		}
	}
	
	/**
	 * Runs the actual BSPJob through Hadoop.
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public boolean run() throws IOException, InterruptedException, 
		ClassNotFoundException {
		setNumReduceTasks(0);
		setJarByClass(BspJob.class);
	    setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
	    return waitForCompletion(true);
	}
}
