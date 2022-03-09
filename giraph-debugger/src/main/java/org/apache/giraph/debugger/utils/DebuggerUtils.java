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
package org.apache.giraph.debugger.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Contains common utility classes shared one or more of:
 * <ul>
 * <li>Graft instrumenter and the
 * <li>server that serves data to Graft GUI by talking to HDFS
 * <li>Wrapper classes around the scenario protocol buffers that are stored
 * under {@link org.apache.giraph.debugger.utils}.
 * </ul>
 *
 * author semihsalihoglu
 */
public class DebuggerUtils {

  /**
   * The path to the HDFS root for storing Graft traces.
   */
  public static final String TRACE_ROOT = System.getProperty(
    "giraph.debugger.traceRootAtHDFS",
    "/user/" + System.getProperty("user.name") + "/giraph-debug-traces");
  /**
   * The path to the HDFS root for storing cached Giraph job jars.
   */
  public static final String JARCACHE_HDFS = System.getProperty(
    "giraph.debugger.jobCacheAtHDFS", TRACE_ROOT + "/jars");
  /**
   * The path to the local root directory for storing cached Giraph job jars.
   */
  public static final String JARCACHE_LOCAL = System.getProperty(
    "giraph.debugger.jobCacheLocal", System.getenv("HOME") +
      "/.giraph-debug/jars");

  /**
   * Enumeration of different trace files Graft saves in HDFS.
   */
  public enum DebugTrace {
    /**
     * Regular trace capturing a vertex computation.
     */
    VERTEX_REGULAR("regular vertex"),
    /**
     * Captured exception from a vertex.
     */
    VERTEX_EXCEPTION("exception from a vertex"),
    /**
     * All traces of a particular vertex.
     */
    VERTEX_ALL,
    /**
     * Captured message integrity violations.
     */
    INTEGRITY_MESSAGE_ALL("invalid messages"),
    /**
     * Trace of the single message violating constraints.
     */
    INTEGRITY_MESSAGE_SINGLE_VERTEX("vertex sending invalid messages"),
    /**
     * Trace of the vertex computation that sends an invalid message.
     */
    INTEGRITY_VERTEX("vertex having invalid value"),
    /**
     * Regular trace of a MasterCompute.
     */
    MASTER_REGULAR("regular MasterCompute"),
    /**
     * Trace capturing exception thrown from a MasterCompute.
     */
    MASTER_EXCEPTION("exception from MasterCompute"),
    /**
     * All traces of MasterCompute.
     */
    MASTER_ALL,
    /**
     * The jar signature that links the instrumented jar.
     */
    JAR_SIGNATURE;

    /**
     * The label of this debug trace.
     */
    private final String label;

    /**
     * Creates a DebugTrace instance without a label.
     */
    private DebugTrace() {
      this.label = null;
    }

    /**
     * Creates a DebugTrace instance with a specific label.
     * @param label The label.
     */
    private DebugTrace(String label) {
      this.label = label;
    }

    /**
     * Returns the label.
     * @return the label
     */
    public String getLabel() {
      return label;
    }
  }

  /**
   * File name prefix for regular traces.
   */
  public static final String PREFIX_TRACE_REGULAR = "reg";
  /**
   * File name prefix for exception traces.
   */
  public static final String PREFIX_TRACE_EXCEPTION = "err";
  /**
   * File name prefix for vertex value integrity traces.
   */
  public static final String PREFIX_TRACE_VERTEX = "vv";
  /**
   * File name prefix for message integrity traces.
   */
  public static final String PREFIX_TRACE_MESSAGE = "msg";

  /**
   * Disallows creating instances of this class.
   */
  private DebuggerUtils() { }

  /**
   * Makes a clone of a writable object. Giraph sometimes reuses and overwrites
   * the bytes inside {@link Writable} objects. For example, when reading the
   * incoming messages inside a {@link Computation} class through the iterator
   * Giraph supplies, Giraph uses only one object. Therefore in order to keep a
   * pointer to particular object, we need to clone it.
   *
   * @param <T>
   *          Type of the clazz.
   * @param writableToClone
   *          Writable object to clone.
   * @param clazz
   *          Class of writableToClone.
   * @return a clone of writableToClone.
   */
  public static <T extends Writable> T makeCloneOf(T writableToClone,
    Class<T> clazz) {
    T idCopy = newInstance(clazz);
    // Return value is null if clazz is assignable to NullWritable.
    if (idCopy == null) {
      return writableToClone;
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(
      byteArrayOutputStream);
    try {
      writableToClone.write(dataOutputStream);
    } catch (IOException e) {
      // Throwing a runtime exception because the methods that call other
      // methods
      // such as addNeighborWrapper or addOutgoingMessageWrapper, implement
      // abstract classes
      // or interfaces of Giraph that we can't edit to include a throws
      // statement.
      throw new RuntimeException(e);
    }
    //
    if (byteArrayOutputStream.toByteArray() != null) {
      WritableUtils.readFieldsFromByteArray(
        byteArrayOutputStream.toByteArray(), idCopy);
      byteArrayOutputStream.reset();
    }
    return idCopy;
  }

  /**
   * Instantiates a new object from the given class.
   *
   * @param <T> The type of the new instance to create.
   * @param theClass The class of the new instance to create.
   * @return The newly created instance.
   */
  public static <T> T newInstance(Class<T> theClass) {
    return NullWritable.class.isAssignableFrom(theClass) ? null :
      ReflectionUtils.newInstance(theClass);
  }

  /**
   * Returns the full trace file name for the given type of debug trace. One or
   * more of the passed arguments will be used in the file name.
   *
   * @param debugTrace The debug trace for generating the file name.
   * @param jobId The job id of the job the debug trace belongs to.
   * @param superstepNo The superstep number of the debug trace.
   * @param vertexId The vertex id of the debug trace.
   * @param taskId The task id of the debug trace.
   * @return The full trace file name.
   */
  public static String getFullTraceFileName(DebugTrace debugTrace,
    String jobId, Long superstepNo, String vertexId, String taskId) {
    return getTraceFileRoot(jobId) + "/" +
      getTraceFileName(debugTrace, superstepNo, vertexId, taskId);
  }

  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   *
   * @param superstepNo The superstep number of the trace.
   * @param jobId The job id of the trace.
   * @param taskId The task id of the trace.
   * @return The full trace file name for debug trace of message integrity.
   */
  public static String getMessageIntegrityAllTraceFullFileName(
    long superstepNo, String jobId, String taskId) {
    return getFullTraceFileName(DebugTrace.INTEGRITY_MESSAGE_ALL, jobId,
      superstepNo, null /* no vertex Id */, taskId);
  }

  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   *
   * @param masterDebugTrace The debug trace for generating the file name.
   * @param jobId The job id the debug trace belongs to.
   * @param superstepNo The superstep number.
   * @return The full trace file name of the master compute trace.
   */
  public static String getFullMasterTraceFileName(DebugTrace masterDebugTrace,
    String jobId, Long superstepNo) {
    return getFullTraceFileName(masterDebugTrace, jobId, superstepNo,
      null /* no vertex Id */, null /* no trace Id */);
  }

  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   *
   * @param debugTrace The debug trace for generating the file name.
   * @param jobId The job id the debug trace belongs to.
   * @param superstepNo The superstep number.
   * @param vertexId The vertex id of the debug trace.
   * @return The full trace file name without the trace id.
   */
  public static String getFullTraceFileName(DebugTrace debugTrace,
    String jobId, Long superstepNo, String vertexId) {
    return getFullTraceFileName(debugTrace, jobId, superstepNo, vertexId,
      null /* no trace Id */);
  }

  /**
   * Maps debug trace to file names with additional parameters.
   *
   * @param debugTrace The debug trace.
   * @param superstepNo The superstep number.
   * @param vertexId The vertex id.
   * @param taskId The task id.
   * @return The file name that corresponds to the debug trace.
   */
  private static String getTraceFileName(DebugTrace debugTrace,
    Long superstepNo, String vertexId, String taskId) {
    String format = getTraceFileFormat(debugTrace);
    switch (debugTrace) {
    case VERTEX_REGULAR:
      return String.format(format, superstepNo, vertexId);
    case VERTEX_EXCEPTION:
      return String.format(format, superstepNo, vertexId);
    case INTEGRITY_MESSAGE_ALL:
      return String.format(format, taskId, superstepNo);
    case INTEGRITY_MESSAGE_SINGLE_VERTEX:
      return String.format(format, superstepNo, vertexId);
    case INTEGRITY_VERTEX:
      return String.format(format, superstepNo, vertexId);
    case MASTER_REGULAR:
      return String.format(format, superstepNo);
    case MASTER_EXCEPTION:
      return String.format(format, superstepNo);
    default:
      return null;
    }
  }

  /**
   * Returns the file name of the trace file given the three parameters. Pass
   * arbitrary vertexId for traces which do not require a vertexId.
   *
   * @param debugTrace
   *          The debug trace.
   * @return The file name format for the debug trace to be used with
   *         {@link String#format(String, Object...)}.
   */
  public static String getTraceFileFormat(DebugTrace debugTrace) {
    // XXX is this function giving the String format? or regex? Seems latter.
    switch (debugTrace) {
    case VERTEX_REGULAR:
      return PREFIX_TRACE_REGULAR + "_stp_%s_vid_%s.tr";
    case VERTEX_EXCEPTION:
      return PREFIX_TRACE_EXCEPTION + "_stp_%s_vid_%s.tr";
    case VERTEX_ALL:
      return String.format("(%s|%s)%s", PREFIX_TRACE_REGULAR,
        PREFIX_TRACE_EXCEPTION, "_stp_%s_vid_%s.tr");
    case INTEGRITY_MESSAGE_ALL:
      return "task_%s_msg_intgrty_stp_%s.tr";
    case INTEGRITY_MESSAGE_SINGLE_VERTEX:
      return PREFIX_TRACE_MESSAGE + "_intgrty_stp_%s_vid_%s.tr";
    case INTEGRITY_VERTEX:
      return PREFIX_TRACE_VERTEX + "_intgrty_stp_%s_vid_%s.tr";
    case MASTER_REGULAR:
      return "master_" + PREFIX_TRACE_REGULAR + "_stp_%s.tr";
    case MASTER_EXCEPTION:
      return "master_" + PREFIX_TRACE_EXCEPTION + "_stp_%s.tr";
    case MASTER_ALL:
      return String.format("master_(%s|%s)_%s", PREFIX_TRACE_REGULAR,
        PREFIX_TRACE_EXCEPTION, "_stp_%s.tr");
    default:
      throw new IllegalArgumentException("DebugTrace not supported.");
    }
  }

  /**
   * Maps prefix back to the corresponding debug trace.
   *
   * @param prefix The file name prefix.
   * @return The debug trace value that corresponds to given prefix.
   * @throws IllegalArgumentException Thrown if prefix isn't supported.
   */
  public static DebugTrace getVertexDebugTraceForPrefix(String prefix) {
    if (prefix.equals(PREFIX_TRACE_REGULAR)) {
      return DebugTrace.VERTEX_REGULAR;
    } else if (prefix.equals(PREFIX_TRACE_EXCEPTION)) {
      return DebugTrace.VERTEX_EXCEPTION;
    } else if (prefix.equals(PREFIX_TRACE_VERTEX)) {
      return DebugTrace.INTEGRITY_VERTEX;
    } else if (prefix.equals(PREFIX_TRACE_MESSAGE)) {
      return DebugTrace.INTEGRITY_MESSAGE_SINGLE_VERTEX;
    } else {
      throw new IllegalArgumentException("Prefix not supported");
    }
  }

  /**
   * Returns the root directory of the trace files for the given job.
   *
   * @param jobId The job id of the job.
   * @return The root path for storing traces for the job.
   */
  public static String getTraceFileRoot(String jobId) {
    return String.format("%s/%s", DebuggerUtils.TRACE_ROOT, jobId);
  }
}
