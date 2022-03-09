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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.debugger.Scenario.CommonVertexMasterContext;
import org.apache.giraph.debugger.Scenario.Exception;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext.Neighbor;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext.OutgoingMessage;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexScenarioClasses;
import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.Scenario.GiraphVertexScenario} protocol
 * buffer. In {@link org.apache.giraph.debugger.Scenario.GiraphVertexScenario}
 * most fields are stored as serialized byte arrays and this class gives them
 * access through the java classes that those byte arrays serialize.
 *
 * @param <I>
 *          vertex ID class.
 * @param <V>
 *          vertex value class.
 * @param <E>
 *          edge value class.
 * @param <M1>
 *          incoming message class.
 * @param <M2>
 *          outgoing message class.
 *
 * author Brian Truong
 */
@SuppressWarnings("rawtypes")
public class GiraphVertexScenarioWrapper<I extends WritableComparable, V extends
  Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
  extends BaseWrapper {

  /**
   * Vertex scenario classes wrapper instance.
   */
  private VertexScenarioClassesWrapper vertexScenarioClassesWrapper = null;
  /**
   * Vertex context wrapper instance.
   */
  private VertexContextWrapper contextWrapper = null;
  /**
   * Exception wrapper instance.
   */
  private ExceptionWrapper exceptionWrapper = null;

  /**
   * Empty constructor to be used for loading from HDFS.
   */
  public GiraphVertexScenarioWrapper() {
  }

  /**
   * Constructor with classes.
   *
   * @param classUnderTest The Computation class under test.
   * @param vertexIdClass The vertex id class.
   * @param vertexValueClass The vertex value class.
   * @param edgeValueClass The edge value class.
   * @param incomingMessageClass The incoming message class.
   * @param outgoingMessageClass The outgoing message class.
   */
  public GiraphVertexScenarioWrapper(
    Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
    Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
    Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
    this.vertexScenarioClassesWrapper = new VertexScenarioClassesWrapper(
      classUnderTest, vertexIdClass, vertexValueClass, edgeValueClass,
      incomingMessageClass, outgoingMessageClass);
    this.contextWrapper = new VertexContextWrapper();
  }

  public VertexContextWrapper getContextWrapper() {
    return contextWrapper;
  }

  public void setContextWrapper(VertexContextWrapper contextWrapper) {
    this.contextWrapper = contextWrapper;
  }

  /**
   * Checks if this has an exception wrapper.
   * @return True if this has an exception wrapper.
   */
  public boolean hasExceptionWrapper() {
    return exceptionWrapper != null;
  }

  public ExceptionWrapper getExceptionWrapper() {
    return exceptionWrapper;
  }

  public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
    this.exceptionWrapper = exceptionWrapper;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\n" + vertexScenarioClassesWrapper.toString());
    stringBuilder.append("\n" + contextWrapper.toString());
    stringBuilder.append("\nhasExceptionWrapper: " + hasExceptionWrapper());
    if (hasExceptionWrapper()) {
      stringBuilder.append("\n" + exceptionWrapper.toString());
    }
    return stringBuilder.toString();
  }

  /**
   * Wrapper class around
   * {@link
   *  org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext}
   * protocol buffer.
   *
   * author semihsalihoglu
   */
  public class VertexContextWrapper extends BaseWrapper {
    /**
     * Vertex master context wrapper instance.
     */
    private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper;
    /**
     * Reference to the vertex id.
     */
    private I vertexIdWrapper;
    /**
     * Reference to the vertex value before the computation.
     */
    private V vertexValueBeforeWrapper;
    /**
     * Reference to the vertex value after the computation.
     */
    private V vertexValueAfterWrapper;
    /**
     * List of incoming messages.
     */
    private ArrayList<M1> inMsgsWrapper;
    /**
     * List of neighbor vertices.
     */
    private ArrayList<NeighborWrapper> neighborsWrapper;
    /**
     * List of outgoing messages.
     */
    private ArrayList<OutgoingMessageWrapper> outMsgsWrapper;

    /**
     * Default constructor.
     */
    public VertexContextWrapper() {
      reset();
    }

    /**
     * Initializes/resets this instances.
     */
    public void reset() {
      this.commonVertexMasterContextWrapper = new
        CommonVertexMasterContextWrapper();
      this.vertexIdWrapper = null;
      this.vertexValueBeforeWrapper = null;
      this.vertexValueAfterWrapper = null;
      this.inMsgsWrapper = new ArrayList<M1>();
      this.neighborsWrapper = new ArrayList<NeighborWrapper>();
      this.outMsgsWrapper = new ArrayList<OutgoingMessageWrapper>();
    }

    public CommonVertexMasterContextWrapper
    getCommonVertexMasterContextWrapper() {
      return commonVertexMasterContextWrapper;
    }

    public void setCommonVertexMasterContextWrapper(
      CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
      this.commonVertexMasterContextWrapper = commonVertexMasterContextWrapper;
    }

    public I getVertexIdWrapper() {
      return vertexIdWrapper;
    }

    public void setVertexIdWrapper(I vertexId) {
      this.vertexIdWrapper = vertexId;
    }

    public V getVertexValueBeforeWrapper() {
      return vertexValueBeforeWrapper;
    }

    public V getVertexValueAfterWrapper() {
      return vertexValueAfterWrapper;
    }

    public void setVertexValueBeforeWrapper(V vertexValueBefore) {
      // Because Giraph does not create new objects for writables, we need
      // to make a clone them to get a copy of the objects. Otherwise, if
      // we call setVertexValueBeforeWrapper and then setVertexValueAfterWrapper
      // both of our copies end up pointing to the same object (in this case to
      // the value passed to setVertexValueAfterWrapper, because it was called
      // later).
      this.vertexValueBeforeWrapper = DebuggerUtils.makeCloneOf(
        vertexValueBefore, getVertexScenarioClassesWrapper().vertexValueClass);
    }

    public void setVertexValueAfterWrapper(V vertexValueAfter) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      this.vertexValueAfterWrapper = DebuggerUtils.makeCloneOf(
        vertexValueAfter, getVertexScenarioClassesWrapper().vertexValueClass);
    }

    /**
     * Captures an incoming message by keeping a clone.
     *
     * @param message The message to capture.
     */
    public void addIncomingMessageWrapper(M1 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      inMsgsWrapper.add(DebuggerUtils.makeCloneOf(message,
        getVertexScenarioClassesWrapper().incomingMessageClass));
    }

    public Collection<M1> getIncomingMessageWrappers() {
      return inMsgsWrapper;
    }

    /**
     * Captures an outgoing message by keeping a clone.
     *
     * @param receiverId The vertex id that receives the message.
     * @param message The message being sent to be captured.
     */
    public void addOutgoingMessageWrapper(I receiverId, M2 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      outMsgsWrapper.add(new OutgoingMessageWrapper(DebuggerUtils.makeCloneOf(
        receiverId, getVertexScenarioClassesWrapper().vertexIdClass),
        DebuggerUtils.makeCloneOf(message,
          getVertexScenarioClassesWrapper().outgoingMessageClass)));
    }

    public Collection<OutgoingMessageWrapper> getOutgoingMessageWrappers() {
      return outMsgsWrapper;
    }

    /**
     * Adds a neighbor vertex.
     *
     * @param neighborId The neighbor vertex id.
     * @param edgeValue The value of the edge that connects to the neighbor.
     */
    public void addNeighborWrapper(I neighborId, E edgeValue) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      neighborsWrapper.add(new NeighborWrapper(DebuggerUtils.makeCloneOf(
        neighborId, getVertexScenarioClassesWrapper().vertexIdClass),
        DebuggerUtils.makeCloneOf(edgeValue,
          getVertexScenarioClassesWrapper().edgeValueClass)));
    }

    public Collection<NeighborWrapper> getNeighborWrappers() {
      return neighborsWrapper;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(commonVertexMasterContextWrapper.toString());
      stringBuilder.append("\nvertexId: " + getVertexIdWrapper());
      stringBuilder.append("\nvertexValueBefore: " +
        getVertexValueBeforeWrapper());
      stringBuilder.append("\nvertexValueAfter: " +
        getVertexValueAfterWrapper());
      stringBuilder.append("\nnumNeighbors: " + getNeighborWrappers().size());

      for (NeighborWrapper neighborWrapper : getNeighborWrappers()) {
        stringBuilder.append("\n" + neighborWrapper.toString());
      }

      for (M1 incomingMesage : getIncomingMessageWrappers()) {
        stringBuilder.append("\nincoming message: " + incomingMesage);
      }

      stringBuilder.append("\nnumOutgoingMessages: " +
        getOutgoingMessageWrappers().size());
      for (OutgoingMessageWrapper outgoingMessageWrapper :
        getOutgoingMessageWrappers()) {
        stringBuilder.append("\n" + outgoingMessageWrapper);
      }
      return stringBuilder.toString();
    }

    /**
     * Wrapper around scenario.giraphscenerio.neighbor (in scenario.proto).
     *
     * author Brian Truong
     */
    public class NeighborWrapper extends BaseWrapper {

      /**
       * Neighbor vertex id.
       */
      private I nbrId;
      /**
       * Value of the edge that points to the neighbor.
       */
      private E edgeValue;

      /**
       * Constructor with the fields.
       *
       * @param nbrId Neighbor vertex id.
       * @param edgeValue Value of the edge that points to the neighbor.
       */
      public NeighborWrapper(I nbrId, E edgeValue) {
        this.nbrId = nbrId;
        this.edgeValue = edgeValue;
      }

      /**
       * Default constructor.
       */
      public NeighborWrapper() {
      }

      public I getNbrId() {
        return nbrId;
      }

      public E getEdgeValue() {
        return edgeValue;
      }

      @Override
      public String toString() {
        return "neighbor: nbrId: " + nbrId + " edgeValue: " + edgeValue;
      }

      @Override
      public GeneratedMessage buildProtoObject() {
        Neighbor.Builder neighborBuilder = Neighbor.newBuilder();
        neighborBuilder.setNeighborId(toByteString(nbrId));
        if (edgeValue != null) {
          neighborBuilder.setEdgeValue(toByteString(edgeValue));
        } else {
          neighborBuilder.clearEdgeValue();
        }
        return neighborBuilder.build();
      }

      @Override
      public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
        throws IOException {
        return Neighbor.parseFrom(inputStream);
      }

      @Override
      public void loadFromProto(GeneratedMessage protoObject)
        throws ClassNotFoundException, IOException, InstantiationException,
        IllegalAccessException {
        Neighbor neighbor = (Neighbor) protoObject;
        this.nbrId = DebuggerUtils
          .newInstance(vertexScenarioClassesWrapper.vertexIdClass);
        fromByteString(neighbor.getNeighborId(), this.nbrId);

        if (neighbor.hasEdgeValue()) {
          this.edgeValue = DebuggerUtils
            .newInstance(vertexScenarioClassesWrapper.edgeValueClass);
          fromByteString(neighbor.getEdgeValue(), this.edgeValue);
        } else {
          this.edgeValue = null;
        }
      }
    }

    /**
     * Class for capturing outgoing message.
     */
    public class OutgoingMessageWrapper extends BaseWrapper {
      /**
       * Destination vertex id.
       */
      private I destinationId;
      /**
       * Outgoing message.
       */
      private M2 message;

      /**
       * Constructor with the field values.
       *
       * @param destinationId Destination vertex id.
       * @param message Outgoing message.
       */
      public OutgoingMessageWrapper(I destinationId, M2 message) {
        this.setDestinationId(destinationId);
        this.setMessage(message);
      }

      /**
       * Default constructor.
       */
      public OutgoingMessageWrapper() {
      }

      public I getDestinationId() {
        return destinationId;
      }

      public M2 getMessage() {
        return message;
      }

      @Override
      public String toString() {
        return "outgoingMessage: destinationId: " + getDestinationId() +
          " message: " + getMessage();
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
          (getDestinationId() == null ? 0 : getDestinationId().hashCode());
        result = prime * result + (getMessage() == null ? 0 :
          getMessage().hashCode());
        return result;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        @SuppressWarnings("unchecked")
        OutgoingMessageWrapper other = (OutgoingMessageWrapper) obj;
        if (getDestinationId() == null) {
          if (other.getDestinationId() != null) {
            return false;
          }
        } else if (!getDestinationId().equals(other.getDestinationId())) {
          return false;
        }
        if (getMessage() == null) {
          if (other.getMessage() != null) {
            return false;
          }
        } else if (!getMessage().equals(other.getMessage())) {
          return false;
        }
        return true;
      }

      @Override
      public GeneratedMessage buildProtoObject() {
        OutgoingMessage.Builder outgoingMessageBuilder = OutgoingMessage
          .newBuilder();
        outgoingMessageBuilder.setMsgData(toByteString(this.getMessage()));
        outgoingMessageBuilder
          .setDestinationId(toByteString(this.getDestinationId()));
        return outgoingMessageBuilder.build();
      }

      @Override
      public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
        throws IOException {
        return OutgoingMessage.parseFrom(inputStream);
      }

      @Override
      public void loadFromProto(GeneratedMessage generatedMessage)
        throws ClassNotFoundException, IOException, InstantiationException,
        IllegalAccessException {
        OutgoingMessage outgoingMessageProto = (OutgoingMessage)
          generatedMessage;
        this.setDestinationId(DebuggerUtils
          .newInstance(getVertexScenarioClassesWrapper().vertexIdClass));
        fromByteString(outgoingMessageProto.getDestinationId(),
          getDestinationId());
        this.setMessage(DebuggerUtils
          .newInstance(getVertexScenarioClassesWrapper().outgoingMessageClass));
        fromByteString(outgoingMessageProto.getMsgData(), this.getMessage());
      }

      /**
       * @param destinationId the destinationId to set
       */
      public void setDestinationId(I destinationId) {
        this.destinationId = destinationId;
      }

      /**
       * @param message the message to set
       */
      public void setMessage(M2 message) {
        this.message = message;
      }
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      VertexContext.Builder contextBuilder = VertexContext.newBuilder();
      contextBuilder
        .setCommonContext((CommonVertexMasterContext)
          commonVertexMasterContextWrapper.buildProtoObject());
      contextBuilder.setVertexId(toByteString(vertexIdWrapper));
      if (vertexValueBeforeWrapper != null) {
        contextBuilder
          .setVertexValueBefore(toByteString(vertexValueBeforeWrapper));
      }
      if (vertexValueAfterWrapper != null) {
        contextBuilder
          .setVertexValueAfter(toByteString(vertexValueAfterWrapper));
      }

      for (GiraphVertexScenarioWrapper<I, V, E, M1, M2>.VertexContextWrapper.
        NeighborWrapper neighborWrapper : neighborsWrapper) {
        contextBuilder.addNeighbor((Neighbor) neighborWrapper
          .buildProtoObject());
      }

      for (M1 msg : inMsgsWrapper) {
        contextBuilder.addInMessage(toByteString(msg));
      }

      for (OutgoingMessageWrapper outgoingMessageWrapper : outMsgsWrapper) {
        contextBuilder.addOutMessage((OutgoingMessage) outgoingMessageWrapper
          .buildProtoObject());
      }

      return contextBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
      throws IOException {
      return VertexContext.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage)
      throws ClassNotFoundException, IOException, InstantiationException,
      IllegalAccessException {
      VertexContext context = (VertexContext) generatedMessage;

      CommonVertexMasterContextWrapper vertexMasterContextWrapper = new
        CommonVertexMasterContextWrapper();
      vertexMasterContextWrapper
        .loadFromProto(context.getCommonContext());
      this.commonVertexMasterContextWrapper = vertexMasterContextWrapper;

      I vertexId = DebuggerUtils
        .newInstance(getVertexScenarioClassesWrapper().vertexIdClass);
      fromByteString(context.getVertexId(), vertexId);
      this.vertexIdWrapper = vertexId;

      V vertexValueBefore = DebuggerUtils
        .newInstance(getVertexScenarioClassesWrapper().vertexValueClass);
      fromByteString(context.getVertexValueBefore(), vertexValueBefore);
      this.vertexValueBeforeWrapper = vertexValueBefore;
      if (context.hasVertexValueAfter()) {
        V vertexValueAfter = DebuggerUtils
          .newInstance(getVertexScenarioClassesWrapper().vertexValueClass);
        fromByteString(context.getVertexValueAfter(), vertexValueAfter);
        this.vertexValueAfterWrapper = vertexValueAfter;
      }

      for (Neighbor neighbor : context.getNeighborList()) {
        NeighborWrapper neighborWrapper = new NeighborWrapper();
        neighborWrapper.loadFromProto(neighbor);
        this.neighborsWrapper.add(neighborWrapper);
      }
      for (int i = 0; i < context.getInMessageCount(); i++) {
        M1 msg = DebuggerUtils
          .newInstance(getVertexScenarioClassesWrapper().incomingMessageClass);
        fromByteString(context.getInMessage(i), msg);
        this.addIncomingMessageWrapper(msg);
      }

      for (OutgoingMessage outgoingMessageProto : context.getOutMessageList()) {
        OutgoingMessageWrapper outgoingMessageWrapper = new
          OutgoingMessageWrapper();
        outgoingMessageWrapper.loadFromProto(outgoingMessageProto);
        this.outMsgsWrapper.add(outgoingMessageWrapper);
      }
    }
  }

  /**
   * Class for capturing the parameter classes used for Giraph Computation.
   */
  public class VertexScenarioClassesWrapper extends
    BaseScenarioAndIntegrityWrapper<I> {
    /**
     * The Computation class.
     */
    private Class<?> classUnderTest;
    /**
     * The vertex value class.
     */
    private Class<V> vertexValueClass;
    /**
     * The edge value class.
     */
    private Class<E> edgeValueClass;
    /**
     * The incoming message class.
     */
    private Class<M1> incomingMessageClass;
    /**
     * The outgoing message class.
     */
    private Class<M2> outgoingMessageClass;

    /**
     * Default constructor.
     */
    public VertexScenarioClassesWrapper() {
    }

    /**
     * Constructor with field values.
     *
     * @param classUnderTest Computation class.
     * @param vertexIdClass Vertex id class.
     * @param vertexValueClass Vertex value class.
     * @param edgeValueClass Edge value class.
     * @param incomingMessageClass Incoming message class.
     * @param outgoingMessageClass Outgoing message class.
     */
    public VertexScenarioClassesWrapper(
      Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
      Class<I> vertexIdClass, Class<V> vertexValueClass,
      Class<E> edgeValueClass, Class<M1> incomingMessageClass,
      Class<M2> outgoingMessageClass) {
      super(vertexIdClass);
      this.classUnderTest = classUnderTest;
      this.vertexValueClass = vertexValueClass;
      this.edgeValueClass = edgeValueClass;
      this.incomingMessageClass = incomingMessageClass;
      this.outgoingMessageClass = outgoingMessageClass;
    }

    public Class<?> getClassUnderTest() {
      return classUnderTest;
    }

    public Class<V> getVertexValueClass() {
      return vertexValueClass;
    }

    public Class<E> getEdgeValueClass() {
      return edgeValueClass;
    }

    public Class<M1> getIncomingMessageClass() {
      return incomingMessageClass;
    }

    public Class<M2> getOutgoingMessageClass() {
      return outgoingMessageClass;
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      VertexScenarioClasses.Builder vertexScenarioClassesBuilder =
        VertexScenarioClasses.newBuilder();
      vertexScenarioClassesBuilder.setClassUnderTest(getClassUnderTest()
        .getName());
      vertexScenarioClassesBuilder.setVertexIdClass(getVertexIdClass()
        .getName());
      vertexScenarioClassesBuilder.setVertexValueClass(getVertexValueClass()
        .getName());
      vertexScenarioClassesBuilder.setEdgeValueClass(getEdgeValueClass()
        .getName());
      vertexScenarioClassesBuilder
        .setIncomingMessageClass(getIncomingMessageClass().getName());
      vertexScenarioClassesBuilder
        .setOutgoingMessageClass(getOutgoingMessageClass().getName());
      return vertexScenarioClassesBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
      throws IOException {
      return VertexScenarioClasses.parseFrom(inputStream);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void loadFromProto(GeneratedMessage generatedMessage)
      throws ClassNotFoundException, IOException, InstantiationException,
      IllegalAccessException {
      VertexScenarioClasses vertexScenarioClass = (VertexScenarioClasses)
        generatedMessage;
      Class<?> clazz = Class.forName(vertexScenarioClass.getClassUnderTest());
      this.classUnderTest = castClassToUpperBound(clazz, Computation.class);
      this.vertexIdClass = (Class<I>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getVertexIdClass()),
        WritableComparable.class);
      this.vertexValueClass = (Class<V>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getVertexValueClass()),
        Writable.class);
      this.edgeValueClass = (Class<E>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getEdgeValueClass()), Writable.class);
      this.incomingMessageClass = (Class<M1>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getIncomingMessageClass()),
        Writable.class);
      this.outgoingMessageClass = (Class<M2>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getOutgoingMessageClass()),
        Writable.class);
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(super.toString());
      stringBuilder.append("\nclassUnderTest: " +
        getClassUnderTest().getCanonicalName());
      stringBuilder.append("\nvertexValueClass: " +
        getVertexValueClass().getCanonicalName());
      stringBuilder.append("\nincomingMessageClass: " +
        getIncomingMessageClass().getCanonicalName());
      stringBuilder.append("\noutgoingMessageClass: " +
        getOutgoingMessageClass().getCanonicalName());
      return stringBuilder.toString();
    }

  }

  @Override
  public void loadFromProto(GeneratedMessage generatedMessage)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    GiraphVertexScenario giraphScenario = (GiraphVertexScenario)
      generatedMessage;
    this.vertexScenarioClassesWrapper = new VertexScenarioClassesWrapper();
    this.vertexScenarioClassesWrapper.loadFromProto(giraphScenario
      .getVertexScenarioClasses());

    this.contextWrapper = new VertexContextWrapper();
    this.contextWrapper.loadFromProto(giraphScenario.getContext());

    if (giraphScenario.hasException()) {
      this.exceptionWrapper = new ExceptionWrapper();
      this.exceptionWrapper.loadFromProto(giraphScenario.getException());
    }
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    GiraphVertexScenario.Builder giraphScenarioBuilder = GiraphVertexScenario
      .newBuilder();
    giraphScenarioBuilder
      .setVertexScenarioClasses((VertexScenarioClasses)
        vertexScenarioClassesWrapper.buildProtoObject());
    giraphScenarioBuilder.setContext((VertexContext) contextWrapper
      .buildProtoObject());
    if (hasExceptionWrapper()) {
      giraphScenarioBuilder.setException((Exception) exceptionWrapper
        .buildProtoObject());
    }
    GiraphVertexScenario giraphScenario = giraphScenarioBuilder.build();
    return giraphScenario;
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return GiraphVertexScenario.parseFrom(inputStream);
  }

  public VertexScenarioClassesWrapper getVertexScenarioClassesWrapper() {
    return vertexScenarioClassesWrapper;
  }

  public void setVertexScenarioClassesWrapper(
    VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
    this.vertexScenarioClassesWrapper = vertexScenarioClassesWrapper;
  }
}
