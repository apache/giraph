/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Class that intercepts call to the AbstractComputation's exposed methods for
 * GiraphDebugger.
 *
 * @param <I>
 *          Vertex id
 * @param <V>
 *          Vertex data
 * @param <E>
 *          Edge data
 * @param <M1>
 *          Incoming message type
 * @param <M2>
 *          Outgoing message type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractInterceptingComputation<
  I extends WritableComparable, V extends Writable, E extends Writable,
  M1 extends Writable, M2 extends Writable>
  extends AbstractComputation<I, V, E, M1, M2> {


  /**
   * Configuration key for the class name of the class that extends DebugConfig.
   */
  public static final String CONFIG_CLASS_KEY = "giraph.debugger.configClass";

  /**
   * Giraph configuration for specifying the DebugConfig class.
   */
  public static final StrConfOption DEBUG_CONFIG_CLASS = new StrConfOption(
    CONFIG_CLASS_KEY, DebugConfig.class.getName(),
    "The name of the Debug Config class for the computation (e.g. " +
      "org.apache.giraph.debugger.examples.SimpleShortestPathsDebugConfig).");

  /**
   * Logger for this class.
   */
  protected static final Logger LOG = Logger
    .getLogger(AbstractInterceptingComputation.class);

  /**
   * A flag to indicate whether this Computation class was already initialized.
   */
  protected static boolean IS_INITIALIZED;
  /**
   * Whether DEBUG_CONFIG tells to check message constraints.
   */
  protected static boolean SHOULD_CHECK_MESSAGE_INTEGRITY;
  /**
   * Whether DEBUG_CONFIG tells to check vertex value constraints.
   */
  protected static boolean SHOULD_CHECK_VERTEX_VALUE_INTEGRITY;
  /**
   * Whether DEBUG_CONFIG tells to catch exceptions.
   */
  protected static boolean SHOULD_CATCH_EXCEPTIONS;

  /**
   * Configuration key for the path to the jar signature.
   */
  private static final String JAR_SIGNATURE_KEY =
    "giraph.debugger.jarSignature";

  /**
   * A constant to limit the number of violations to log.
   */
  private static int NUM_VIOLATIONS_TO_LOG = 5;
  /**
   * A constant to limit the number of vertices to log.
   */
  private static int NUM_VERTICES_TO_LOG = 5;
  /**
   * A counter for number of vertices already logged.
   */
  private static int NUM_VERTICES_LOGGED = 0;
  /**
   * A counter for number of vertex violations already logged.
   */
  private static int NUM_VERTEX_VIOLATIONS_LOGGED = -1;
  /**
   * A counter for number of message violations already logged.
   */
  private static int NUM_MESSAGE_VIOLATIONS_LOGGED = -1;

  /**
   * DebugConfig instance to be used for debugging.
   */
  private static DebugConfig DEBUG_CONFIG;

  /**
   * The vertex id type as in the I of Giraph's Computation&lt;I,V,E,M1,M2>.
   */
  private static Type VERTEX_ID_CLASS;
  /**
   * The vertex value type as in the V of Giraph's Computation&lt;I,V,E,M1,M2>.
   */
  private static Type VERTEX_VALUE_CLASS;
  /**
   * The edge value type as in the E of Giraph's Computation&lt;I,V,E,M1,M2>.
   */
  private static Type EDGE_VALUE_CLASS;
  /**
   * The incoming message type as in the M1 of Giraph's
   * Computation&lt;I,V,E,M1,M2>.
   */
  private static Type INCOMING_MESSAGE_CLASS;
  /**
   * The outgoing message type as in the M2 of Giraph's
   * Computation&lt;I,V,E,M1,M2>.
   */
  private static Type OUTGOING_MESSAGE_CLASS;

  /**
   * Contains previous aggregators that are available in the beginning of the
   * superstep.In Giraph, these aggregators are immutable. NOTE: We currently
   * only capture aggregators that are read by at least one vertex. If we want
   * to capture all aggregators we need to change Giraph code to be get access
   * to them.
   */
  private static CommonVertexMasterInterceptionUtil
  COMMON_VERTEX_MASTER_INTERCEPTING_UTIL;

  /**
   * Whether or not this vertex was configured to be debugged. If so we will
   * intercept its outgoing messages.
   */
  private boolean shouldDebugVertex;
  /**
   * Whether to stop intercepting compute() for the remaining vertices.
   */
  private boolean shouldStopInterceptingVertex;

  /**
   * For vertices that are configured to be debugged, we construct a
   * GiraphVertexScenarioWrapper in the beginning and use it to intercept
   * outgoing messages
   */
  private GiraphVertexScenarioWrapper<I, V, E, M1, M2>
  giraphVertexScenarioWrapperForRegularTraces;

  /**
   * If a vertex has violated a message value constraint when it was sending a
   * message we set this to true so that at the inside interceptComputeEnd()
   * method we make sure we save a vertexScenario trace for it.
   */
  private boolean hasViolatedMsgValueConstraint;
  /**
   * Stores the value of a vertex before the compute method is called. If a
   * vertex throws an exception, or violates a vertex or message value
   * constraint, then we use this value as the previous vertex value when we
   * save a vertexScenario trace for it.
   */
  private V previousVertexValue;
  /**
   * DataOutputBuffer for holding the previous vertex value.
   */
  private DataOutputBuffer previousVertexValueOutputBuffer =
    new DataOutputBuffer();
  /**
   * DataInputBuffer for cloning what was preserved for previous vertex value.
   */
  private DataInputBuffer previousVertexValueInputBuffer =
    new DataInputBuffer();
  /**
   * We keep the vertex under compute in case some functions need it, e.g.,
   * sendMessage().
   */
  private Vertex<I, V, E> currentVertexUnderCompute;
  /**
   * The wrapped instance of message integrity violation.
   */
  private MsgIntegrityViolationWrapper<I, M2> msgIntegrityViolationWrapper;

  /**
   * Provides a way to access the actual Computation class.
   * @return The actual Computation class
   */
  public abstract Class<? extends Computation<I, V, E, ? extends Writable,
    ? extends Writable>> getActualTestedClass();

  /**
   * Initializes this class to start debugging.
   */
  protected final synchronized void
  initializeAbstractInterceptingComputation() {
    if (IS_INITIALIZED) {
      return; // don't initialize twice
    }
    IS_INITIALIZED = true;
    COMMON_VERTEX_MASTER_INTERCEPTING_UTIL =
      new CommonVertexMasterInterceptionUtil(
        getContext().getJobID().toString());
    String debugConfigClassName = DEBUG_CONFIG_CLASS.get(getConf());
    LOG.info("initializing debugConfigClass: " + debugConfigClassName);
    Class<?> clazz;
    try {
      clazz = Class.forName(debugConfigClassName);
      DEBUG_CONFIG = (DebugConfig<I, V, E, M1, M2>) clazz.newInstance();
      DEBUG_CONFIG.readConfig(getConf(), getTotalNumVertices(),
        getContext().getJobID().getId());
      VERTEX_ID_CLASS = getConf().getVertexIdClass();
      VERTEX_VALUE_CLASS = getConf().getVertexValueClass();
      EDGE_VALUE_CLASS = getConf().getEdgeValueClass();
      INCOMING_MESSAGE_CLASS = getConf().getIncomingMessageValueClass();
      OUTGOING_MESSAGE_CLASS = getConf().getOutgoingMessageValueClass();
      // Set limits from DebugConfig
      NUM_VERTICES_TO_LOG = DEBUG_CONFIG.getNumberOfVerticesToLog();
      NUM_VIOLATIONS_TO_LOG = DEBUG_CONFIG.getNumberOfViolationsToLog();
      // Reset counters
      NUM_MESSAGE_VIOLATIONS_LOGGED = 0;
      NUM_VERTEX_VIOLATIONS_LOGGED = 0;
      NUM_VERTICES_LOGGED = 0;
      // Cache DebugConfig flags
      SHOULD_CATCH_EXCEPTIONS = DEBUG_CONFIG.shouldCatchExceptions();
      SHOULD_CHECK_VERTEX_VALUE_INTEGRITY =
        DEBUG_CONFIG.shouldCheckVertexValueIntegrity();
      SHOULD_CHECK_MESSAGE_INTEGRITY =
        DEBUG_CONFIG.shouldCheckMessageIntegrity();
    } catch (InstantiationException | ClassNotFoundException |
      IllegalAccessException e) {
      LOG.error("Could not create a new DebugConfig instance of " +
        debugConfigClassName);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    if (getWorkerContext().getMyWorkerIndex() == getWorkerContext()
      .getWorkerCount() - 1) {
      // last worker records jar signature if necessary
      String jarSignature = getConf().get(JAR_SIGNATURE_KEY);
      if (jarSignature != null) {
        Path jarSignaturePath = new Path(
          DebuggerUtils.getTraceFileRoot(COMMON_VERTEX_MASTER_INTERCEPTING_UTIL
            .getJobId()) + "/" + "jar.signature");
        LOG.info("Recording jar signature (" + jarSignature + ") at " +
          jarSignaturePath);
        FileSystem fs = COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.getFileSystem();
        try {
          if (!fs.exists(jarSignaturePath)) {
            OutputStream f = fs.create(jarSignaturePath,
              true).getWrappedStream();
            IOUtils.write(jarSignature, f);
            f.close();
          }
        } catch (IOException e) {
          // When multiple workers try to write the jar.signature, some of them
          // may cause
          // AlreadyBeingCreatedException to be thrown, which we ignore.
          e.printStackTrace();
        }
      }
    }
    LOG.info("done initializing debugConfigClass: " + debugConfigClassName);
  }

  /**
   * Keep the vertex value as the previous one.
   *
   * @param vertex the vertex
   * @throws IOException
   */
  private void keepPreviousVertexValue(Vertex<I, V, E> vertex) throws
  IOException {
    previousVertexValueOutputBuffer.reset();
    vertex.getValue().write(previousVertexValueOutputBuffer);
  }

  /**
   * Clone the kept previous vertex value.
   *
   * @return Copy of previous vertex value.
   *   Same instance will be reused across multiple calls.
   * @throws IOException
   */
  private V getPreviousVertexValue() throws IOException {
    previousVertexValueInputBuffer.reset(
      previousVertexValueOutputBuffer.getData(),
      previousVertexValueOutputBuffer.getLength());
    if (previousVertexValue == null) {
      previousVertexValue = getConf().createVertexValue();
    }
    previousVertexValue.readFields(previousVertexValueInputBuffer);
    return previousVertexValue;
  }

  /**
   * @return whether captured enough number of info for debugging.
   */
  private boolean hasInterceptedEnough() {
    return NUM_VERTICES_LOGGED >= NUM_VERTICES_TO_LOG ||
      NUM_VERTEX_VIOLATIONS_LOGGED >= NUM_VIOLATIONS_TO_LOG ||
      NUM_MESSAGE_VIOLATIONS_LOGGED >= NUM_VIOLATIONS_TO_LOG;
  }

  /**
   * Called before {@link Computation#preSuperstep()} to prepare a message
   * integrity violation wrapper.
   * @return true if compute() does not need to be intercepted for this
   *  superstep.
   */
  protected final boolean interceptPreSuperstepBegin() {
    // LOG.info("before preSuperstep");
    NUM_VERTICES_LOGGED = 0;
    NUM_VERTEX_VIOLATIONS_LOGGED = 0;
    NUM_MESSAGE_VIOLATIONS_LOGGED = 0;
    if (!DEBUG_CONFIG.shouldDebugSuperstep(getSuperstep()) ||
      hasInterceptedEnough()) {
      shouldStopInterceptingVertex = true;
      return true;
    }
    if (SHOULD_CHECK_VERTEX_VALUE_INTEGRITY) {
      LOG.info("creating a vertexValueViolationWrapper. superstepNo: " +
        getSuperstep());
    }

    if (SHOULD_CHECK_MESSAGE_INTEGRITY) {
      LOG.info("creating a msgIntegrityViolationWrapper. superstepNo: " +
        getSuperstep());
      msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper<>(
        (Class<I>) VERTEX_ID_CLASS, (Class<M2>) OUTGOING_MESSAGE_CLASS);
      msgIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }

    // LOG.info("before preSuperstep done");
    shouldStopInterceptingVertex = false;
    return false;
  }

  /**
   * Called immediately when the compute() method is entered. Initializes data
   * that will be required for debugging throughout the rest of the compute
   * function.
   *
   * @param vertex The vertex that's about to be computed.
   * @param messages The incoming messages for the vertex.
   * @throws IOException
   */
  protected final void interceptComputeBegin(Vertex<I, V, E> vertex,
    Iterable<M1> messages) throws IOException {
    if (!IS_INITIALIZED) {
      // TODO: Sometimes Giraph doesn't call initialize() and directly calls
      // compute(). Here we
      // guard against things not being initiliazed, which was causing null
      // pointer exceptions.
      // Find out when/why this happens.
      LOG.warn("interceptComputeBegin is called but debugConfig is null." +
        " Initializing AbstractInterceptingComputation again...");
      initializeAbstractInterceptingComputation();
    }
    // A vertex should be debugged if:
    // 1) the user configures the superstep to be debugged;
    // 2) the user configures the vertex to be debugged; and
    // 3) we have already debugged less than a threshold of vertices in this
    // superstep.
    shouldDebugVertex = NUM_VERTICES_LOGGED < NUM_VERTICES_TO_LOG &&
      DEBUG_CONFIG.shouldDebugVertex(vertex, getSuperstep());
    if (shouldDebugVertex) {
      giraphVertexScenarioWrapperForRegularTraces = getGiraphVertexScenario(
        vertex, vertex.getValue(), messages);
    }
    // Keep a reference to the current vertex only when necessary.
    if (SHOULD_CHECK_MESSAGE_INTEGRITY &&
      NUM_MESSAGE_VIOLATIONS_LOGGED < NUM_VIOLATIONS_TO_LOG) {
      currentVertexUnderCompute = vertex;
      hasViolatedMsgValueConstraint = false;
    }
    // Keep the previous value only when necessary.
    if (SHOULD_CATCH_EXCEPTIONS ||
      SHOULD_CHECK_VERTEX_VALUE_INTEGRITY &&
      NUM_VERTEX_VIOLATIONS_LOGGED < NUM_VIOLATIONS_TO_LOG ||
      SHOULD_CHECK_MESSAGE_INTEGRITY &&
      NUM_MESSAGE_VIOLATIONS_LOGGED < NUM_VIOLATIONS_TO_LOG) {
      keepPreviousVertexValue(vertex);
    }
  }

  /**
   * Captures exception from {@link Computation#compute(Vertex, Iterable)}.
   *
   * @param vertex The vertex that was being computed.
   * @param messages The incoming messages for the vertex.
   * @param e The exception thrown.
   * @throws IOException
   */
  protected final void interceptComputeException(Vertex<I, V, E> vertex,
    Iterable<M1> messages, Throwable e) throws IOException {
    LOG.info("Caught an exception. message: " + e.getMessage() +
      ". Saving a trace in HDFS.");
    GiraphVertexScenarioWrapper<I, V, E, M1, M2>
    giraphVertexScenarioWrapperForExceptionTrace = getGiraphVertexScenario(
      vertex, getPreviousVertexValue(), messages);
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphVertexScenarioWrapperForExceptionTrace
      .setExceptionWrapper(exceptionWrapper);
    COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.saveScenarioWrapper(
      giraphVertexScenarioWrapperForExceptionTrace, DebuggerUtils
        .getFullTraceFileName(DebugTrace.VERTEX_EXCEPTION,
          COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.getJobId(), getSuperstep(),
          vertex.getId().toString()));
  }

  /**
   * Called after {@link Computation#compute(Vertex, Iterable)} to check vertex
   * and message value integrity.
   *
   * @param vertex The vertex that was computed.
   * @param messages The incoming messages for the vertex.
   * @return whether compute() needs to be intercepted more.
   * @throws IOException
   */
  protected final boolean interceptComputeEnd(Vertex<I, V, E> vertex,
    Iterable<M1> messages) throws IOException {
    if (shouldDebugVertex) {
      // Reflect changes made by compute to scenario.
      giraphVertexScenarioWrapperForRegularTraces.getContextWrapper()
        .setVertexValueAfterWrapper(vertex.getValue());
      // Save vertex scenario.
      COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.saveScenarioWrapper(
        giraphVertexScenarioWrapperForRegularTraces, DebuggerUtils
          .getFullTraceFileName(DebugTrace.VERTEX_REGULAR,
            COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.getJobId(), getSuperstep(),
            vertex.getId().toString()));
      NUM_VERTICES_LOGGED++;
    }
    if (SHOULD_CHECK_VERTEX_VALUE_INTEGRITY &&
      NUM_VERTEX_VIOLATIONS_LOGGED < NUM_VIOLATIONS_TO_LOG &&
      !DEBUG_CONFIG.isVertexValueCorrect(vertex.getId(), vertex.getValue())) {
      initAndSaveGiraphVertexScenarioWrapper(vertex, messages,
        DebugTrace.INTEGRITY_VERTEX);
      NUM_VERTEX_VIOLATIONS_LOGGED++;
    }
    if (hasViolatedMsgValueConstraint) {
      initAndSaveGiraphVertexScenarioWrapper(vertex, messages,
        DebugTrace.INTEGRITY_MESSAGE_SINGLE_VERTEX);
      NUM_MESSAGE_VIOLATIONS_LOGGED++;
    }

    shouldStopInterceptingVertex = hasInterceptedEnough();
    return shouldStopInterceptingVertex;
  }

  /**
   * Called after {@link Computation#postSuperstep()} to save the captured
   * scenario.
   */
  protected final void interceptPostSuperstepEnd() {
    // LOG.info("after postSuperstep");
    if (SHOULD_CHECK_MESSAGE_INTEGRITY &&
      msgIntegrityViolationWrapper.numMsgWrappers() > 0) {
      COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.saveScenarioWrapper(
        msgIntegrityViolationWrapper, DebuggerUtils
          .getMessageIntegrityAllTraceFullFileName(getSuperstep(),
            COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.getJobId(), UUID.randomUUID()
              .toString()));
    }
    // LOG.info("after postSuperstep done");
  }

  /**
   * Saves the captured scenario for the given vertex.
   *
   * @param vertex The vertex that was computed.
   * @param messages The incoming messages for the vertex.
   * @param debugTrace The debug trace to save.
   * @throws IOException
   */
  private void initAndSaveGiraphVertexScenarioWrapper(Vertex<I, V, E> vertex,
    Iterable<M1> messages, DebugTrace debugTrace) throws IOException {
    GiraphVertexScenarioWrapper<I, V, E, M1, M2>
    giraphVertexScenarioWrapper = getGiraphVertexScenario(
      vertex, getPreviousVertexValue(), messages);
    COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.saveScenarioWrapper(
      giraphVertexScenarioWrapper, DebuggerUtils.getFullTraceFileName(
        debugTrace, COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.getJobId(),
        getSuperstep(), vertex.getId().toString()));
  }

  /**
   * We pass the previous vertex value to assign as an argument because for some
   * traces we capture the context lazily and store the previous value
   * temporarily in an object. In those cases the previous value is not equal to
   * the current value of the vertex. And sometimes it is equal to the current
   * value.
   *
   * @param vertex The vertex the scenario will capture.
   * @param previousVertexValueToAssign The previous vertex value.
   * @param messages The incoming messages for this superstep.
   * @return A scenario for the given vertex.
   * @throws IOException
   */
  private GiraphVertexScenarioWrapper<I, V, E, M1, M2> getGiraphVertexScenario(
    Vertex<I, V, E> vertex, V previousVertexValueToAssign,
    Iterable<M1> messages) throws IOException {
    GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapper =
      new GiraphVertexScenarioWrapper(
        getActualTestedClass(), (Class<I>) VERTEX_ID_CLASS,
        (Class<V>) VERTEX_VALUE_CLASS, (Class<E>) EDGE_VALUE_CLASS,
        (Class<M1>) INCOMING_MESSAGE_CLASS, (Class<M2>) OUTGOING_MESSAGE_CLASS);
    VertexContextWrapper contextWrapper =
      giraphVertexScenarioWrapper.getContextWrapper();
    contextWrapper
      .setVertexValueBeforeWrapper(previousVertexValueToAssign);
    COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.initCommonVertexMasterContextWrapper(
      getConf(), getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    contextWrapper
      .setCommonVertexMasterContextWrapper(
        COMMON_VERTEX_MASTER_INTERCEPTING_UTIL
        .getCommonVertexMasterContextWrapper());
    giraphVertexScenarioWrapper.getContextWrapper().setVertexIdWrapper(
      vertex.getId());
    Iterable<Edge<I, E>> returnVal = vertex.getEdges();
    for (Edge<I, E> edge : returnVal) {
      giraphVertexScenarioWrapper.getContextWrapper().addNeighborWrapper(
        edge.getTargetVertexId(), edge.getValue());
    }
    for (M1 message : messages) {
      giraphVertexScenarioWrapper.getContextWrapper()
        .addIncomingMessageWrapper(message);
    }
    giraphVertexScenarioWrapper.getContextWrapper().setVertexValueAfterWrapper(
      vertex.getValue());
    return giraphVertexScenarioWrapper;
  }

  /**
   * First intercepts the sent message if necessary and calls and then calls
   * AbstractComputation's sendMessage method.
   *
   * @param id
   *          Vertex id to send the message to
   * @param message
   *          Message data to send
   */
  @Override
  public void sendMessage(I id, M2 message) {
    if (!shouldStopInterceptingVertex) {
      if (shouldDebugVertex) {
        giraphVertexScenarioWrapperForRegularTraces.getContextWrapper()
          .addOutgoingMessageWrapper(id, message);
      }
      if (SHOULD_CHECK_MESSAGE_INTEGRITY &&
        NUM_MESSAGE_VIOLATIONS_LOGGED < NUM_VIOLATIONS_TO_LOG) {
        I senderId = currentVertexUnderCompute.getId();
        if (!DEBUG_CONFIG.isMessageCorrect(senderId, id, message,
          getSuperstep())) {
          msgIntegrityViolationWrapper.addMsgWrapper(
            currentVertexUnderCompute.getId(), id, message);
          NUM_MESSAGE_VIOLATIONS_LOGGED++;
          hasViolatedMsgValueConstraint = true;
        }
      }
    }
    super.sendMessage(id, message);
  }

  /**
   * First intercepts the sent messages to all edges if necessary and calls and
   * then calls AbstractComputation's sendMessageToAllEdges method.
   *
   * @param vertex
   *          Vertex whose edges to send the message to.
   * @param message
   *          Message sent to all edges.
   */
  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    if (!shouldStopInterceptingVertex) {
      if (shouldDebugVertex) {
        for (Edge<I, E> edge : vertex.getEdges()) {
          giraphVertexScenarioWrapperForRegularTraces.getContextWrapper()
            .addOutgoingMessageWrapper(edge.getTargetVertexId(), message);
        }
      }
      if (SHOULD_CHECK_MESSAGE_INTEGRITY) {
        I senderId = vertex.getId();
        for (Edge<I, E> edge : vertex.getEdges()) {
          if (NUM_MESSAGE_VIOLATIONS_LOGGED >= NUM_VIOLATIONS_TO_LOG) {
            break;
          }
          I id = edge.getTargetVertexId();
          if (DEBUG_CONFIG.isMessageCorrect(senderId, id, message,
            getSuperstep())) {
            continue;
          }
          msgIntegrityViolationWrapper.addMsgWrapper(senderId, id, message);
          hasViolatedMsgValueConstraint = true;
          NUM_MESSAGE_VIOLATIONS_LOGGED++;
        }
      }
    }
    super.sendMessageToAllEdges(vertex, message);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    A retVal = super.<A>getAggregatedValue(name);
    if (!shouldStopInterceptingVertex) {
      COMMON_VERTEX_MASTER_INTERCEPTING_UTIL.addAggregatedValueIfNotExists(name,
        retVal);
    }
    return retVal;
  }
}
