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
import java.util.List;

import org.apache.giraph.debugger.Integrity.MessageIntegrityViolation;
import org.apache.giraph.debugger.Integrity.MessageIntegrityViolation.ExtendedOutgoingMessage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.GeneratedMessage;

/**
 * A wrapper class around the contents of MessageIntegrityViolation inside
 * integrity.proto. In scenario.proto most things are stored as serialized byte
 * arrays and this class gives them access through the java classes that those
 * byte arrays serialize.
 *
 * @param <I>
 *          vertex ID class.
 * @param <M2>
 *          outgoing message class.
 *
 * author Semih Salihoglu
 */
@SuppressWarnings("rawtypes")
public class MsgIntegrityViolationWrapper<I extends WritableComparable,
  M2 extends Writable>
  extends BaseScenarioAndIntegrityWrapper<I> {

  /**
   * Outgoing message class.
   */
  private Class<M2> outgoingMessageClass;
  /**
   * List of captured outgoing messages.
   */
  private final List<ExtendedOutgoingMessageWrapper>
  extendedOutgoingMessageWrappers = new ArrayList<>();
  /**
   * The superstep number at which these message violations were found.
   */
  private long superstepNo;

  /**
   * Empty constructor to be used for loading from HDFS.
   */
  public MsgIntegrityViolationWrapper() {
  }

  /**
   * Constructor with field values.
   *
   * @param vertexIdClass Vertex id class.
   * @param outgoingMessageClass Outgoing message class.
   */
  public MsgIntegrityViolationWrapper(Class<I> vertexIdClass,
    Class<M2> outgoingMessageClass) {
    initialize(vertexIdClass, outgoingMessageClass);
  }

  /**
   * Initializes this instance.
   *
   * @param vertexIdClass Vertex id class.
   * @param outgoingMessageClass Outgoing message class.
   */
  private void initialize(Class<I> vertexIdClass, Class<M2>
  outgoingMessageClass) {
    super.initialize(vertexIdClass);
    this.outgoingMessageClass = outgoingMessageClass;
  }

  public Collection<ExtendedOutgoingMessageWrapper>
  getExtendedOutgoingMessageWrappers() {
    return extendedOutgoingMessageWrappers;
  }

  /**
   * Captures an outgoing message.
   *
   * @param srcId Sending vertex id.
   * @param destinationId Receiving vertex id.
   * @param message The message being sent to capture.
   */
  public void addMsgWrapper(I srcId, I destinationId, M2 message) {
    extendedOutgoingMessageWrappers.add(new ExtendedOutgoingMessageWrapper(
      DebuggerUtils.makeCloneOf(srcId, vertexIdClass), DebuggerUtils
        .makeCloneOf(destinationId, vertexIdClass), DebuggerUtils.makeCloneOf(
          message, outgoingMessageClass)));
  }

  /**
   * @return The number of captured messages so far.
   */
  public int numMsgWrappers() {
    return extendedOutgoingMessageWrappers.size();
  }

  public Class<M2> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\noutgoingMessageClass: " +
      getOutgoingMessageClass().getCanonicalName());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper :
      getExtendedOutgoingMessageWrappers()) {
      stringBuilder.append("\n" + extendedOutgoingMessageWrapper);
    }
    return stringBuilder.toString();
  }

  /**
   * Class for capturing outgoing messages as well as the sending vertex id.
   */
  public class ExtendedOutgoingMessageWrapper extends BaseWrapper {
    /**
     * Sending vertex id.
     */
    private I srcId;
    /**
     * Receiving vertex id.
     */
    private I destinationId;
    /**
     * Message being sent.
     */
    private M2 message;

    /**
     * Constructor with field values.
     *
     * @param srcId Sending vertex id.
     * @param destinationId Receiving vertex id.
     * @param message Message being sent.
     */
    public ExtendedOutgoingMessageWrapper(I srcId, I destinationId, M2 message)
    {
      this.setSrcId(srcId);
      this.setDestinationId(destinationId);
      this.setMessage(message);
    }

    /**
     * Default constructor.
     */
    public ExtendedOutgoingMessageWrapper() {
    }

    @Override
    public String toString() {
      return "extendedOutgoingMessage: srcId: " + getSrcId() +
        " destinationId: " + getDestinationId() + " message: " + getMessage();
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      ExtendedOutgoingMessage.Builder extendedOutgoingMessageBuilder =
        ExtendedOutgoingMessage.newBuilder();
      extendedOutgoingMessageBuilder.setSrcId(toByteString(getSrcId()));
      extendedOutgoingMessageBuilder
        .setDestinationId(toByteString(getDestinationId()));
      extendedOutgoingMessageBuilder.setMsgData(toByteString(getMessage()));
      return extendedOutgoingMessageBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
      throws IOException {
      return ExtendedOutgoingMessage.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage)
      throws ClassNotFoundException, IOException, InstantiationException,
      IllegalAccessException {
      ExtendedOutgoingMessage extendedOutgoingMessage =
        (ExtendedOutgoingMessage) generatedMessage;
      this.setSrcId(DebuggerUtils.newInstance(vertexIdClass));
      fromByteString(extendedOutgoingMessage.getSrcId(), this.getSrcId());
      this.setDestinationId(DebuggerUtils.newInstance(vertexIdClass));
      fromByteString(extendedOutgoingMessage.getDestinationId(),
        this.getDestinationId());
      this.setMessage(DebuggerUtils.newInstance(outgoingMessageClass));
      fromByteString(extendedOutgoingMessage.getMsgData(), this.getMessage());
    }

    /**
     * @return the srcId
     */
    public I getSrcId() {
      return srcId;
    }

    /**
     * @param srcId the srcId to set
     */
    public void setSrcId(I srcId) {
      this.srcId = srcId;
    }

    /**
     * @return the destinationId
     */
    public I getDestinationId() {
      return destinationId;
    }

    /**
     * @param destinationId the destinationId to set
     */
    public void setDestinationId(I destinationId) {
      this.destinationId = destinationId;
    }

    /**
     * @return the message
     */
    public M2 getMessage() {
      return message;
    }

    /**
     * @param message the message to set
     */
    public void setMessage(M2 message) {
      this.message = message;
    }
  }

  public long getSuperstepNo() {
    return superstepNo;
  }

  public void setSuperstepNo(long superstepNo) {
    this.superstepNo = superstepNo;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    MessageIntegrityViolation.Builder messageIntegrityViolationBuilder =
      MessageIntegrityViolation.newBuilder();
    messageIntegrityViolationBuilder.setVertexIdClass(getVertexIdClass()
      .getName());
    messageIntegrityViolationBuilder
      .setOutgoingMessageClass(getOutgoingMessageClass().getName());
    messageIntegrityViolationBuilder.setSuperstepNo(getSuperstepNo());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper :
      extendedOutgoingMessageWrappers) {
      messageIntegrityViolationBuilder
        .addMessage((ExtendedOutgoingMessage) extendedOutgoingMessageWrapper
          .buildProtoObject());
    }
    return messageIntegrityViolationBuilder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void loadFromProto(GeneratedMessage generatedMessage)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    MessageIntegrityViolation msgIntegrityViolation =
      (MessageIntegrityViolation) generatedMessage;
    Class<I> vertexIdClass = (Class<I>) castClassToUpperBound(
      Class.forName(msgIntegrityViolation.getVertexIdClass()),
      WritableComparable.class);

    Class<M2> outgoingMessageClazz = (Class<M2>) castClassToUpperBound(
      Class.forName(msgIntegrityViolation.getOutgoingMessageClass()),
      Writable.class);

    initialize(vertexIdClass, outgoingMessageClazz);
    setSuperstepNo(msgIntegrityViolation.getSuperstepNo());

    for (ExtendedOutgoingMessage extendOutgoingMessage : msgIntegrityViolation
      .getMessageList()) {
      ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper = new
        ExtendedOutgoingMessageWrapper();
      extendedOutgoingMessageWrapper.loadFromProto(extendOutgoingMessage);
      extendedOutgoingMessageWrappers.add(extendedOutgoingMessageWrapper);
    }
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return MessageIntegrityViolation.parseFrom(inputStream);
  }
}
