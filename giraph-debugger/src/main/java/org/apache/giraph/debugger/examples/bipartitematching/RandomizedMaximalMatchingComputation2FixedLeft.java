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
package org.apache.giraph.debugger.examples.bipartitematching;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Randomized maximal bipartite graph matching algorithm implementation. It
 * assumes all vertices whose ids are even are in the left part, and odd in the
 * right.
 */
public class RandomizedMaximalMatchingComputation2FixedLeft extends
  BasicComputation<LongWritable, VertexValue, NullWritable, Message> {

  @Override
  public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex,
    Iterable<Message> messages) throws IOException {

    int phase = (int) (getSuperstep() % 4);
    switch (phase) {
    case 0: // "In phase 0 of a cycle,"
      // "each left vertex not yet matched"
      if (isLeft(vertex)) {
        if (isNotMatchedYet(vertex)) {
          // "sends a message to each of its neighbors to request a match,"
          sendMessageToAllEdges(vertex, createRequestMessage(vertex));
          // "and then unconditionally votes to halt."
          vertex.voteToHalt();
        }
      }
      // "If it sent no messages (because it is already matched, or has no
      // outgoing edges), or if all the message recipients are already
      // matched, it will never be reactivated. Otherwise, it will receive a
      // response in two supersteps and reactivate."
      break;

    case 1: // "In phase 1 of a cycle,"
      // "each right vertex not yet matched"
      if (isRight(vertex)) {
        if (isNotMatchedYet(vertex)) {
          int i = 0;
          for (Message msg : messages) {
            // "randomly chooses one of the messages it receives,"
            Message reply = (i == 0) ? // (by simply granting the first one)
              // "sends a message granting that request, and"
              createGrantingMessage(vertex) :
              // "sends messages to other requestors denying it."
              createDenyingMessage(vertex);
            sendMessage(new LongWritable(msg.getSenderVertex()), reply);
            ++i;
          }
          // "Then it unconditionally votes to halt."
          vertex.voteToHalt(); // XXX It is ambiguous if only unmatched right
                               // vertices must halt, or all right ones must.
        }
      }
      break;

    case 2: // "In phase 2 of a cycle,"
      // "each left vertex not yet matched"
      if (isLeft(vertex)) {
        if (isNotMatchedYet(vertex)) {
          // "chooses one of the grants it receives"
          for (Message msg : messages) {
            if (msg.isGranting()) {
              // (by simply picking the first one)
              // "and sends an acceptance message."
              sendMessage(new LongWritable(msg.getSenderVertex()),
                createGrantingMessage(vertex));
              // (and also record which vertex was matched)
              vertex.getValue().setMatchedVertex(msg.getSenderVertex());
              vertex.voteToHalt();    // XXX (Not in the original text)
                                      // Unless matched left vertices halt,
                                      // program ends prematurely.
              break;
            }
          }
          // "Left vertices that are already matched will never execute this
          // phase, since they will not have sent a message in phase 0."
        }
      }
      break;

    case 3: // "Finally, in phase 3,"
      // "an unmatched right vertex"
      if (isRight(vertex)) {
        if (isNotMatchedYet(vertex)) {
          // "receives at most one acceptance message."
          for (Message msg : messages) {
            // "It notes the matched node"
            vertex.getValue().setMatchedVertex(msg.getSenderVertex());
            break;
          }
          // "and unconditionally votes to halt"
          vertex.voteToHalt(); // XXX Again, it's ambiguous if only unmatched
                               // right vertices must halt, or all right ones
                               // must.
          // "it has nothing further to do."
        }
      }
      break;

    default:
      throw new IllegalStateException("No such phase " + phase);
    }
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex belongs to the left part
   */
  boolean isLeft(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return vertex.getId().get() % 2 == 1;
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex belongs to the right part
   */
  boolean isRight(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return !isLeft(vertex);
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex has a match
   */
  private boolean isNotMatchedYet(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return !vertex.getValue().isMatched();
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message requesting a match
   */
  private Message createRequestMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex);
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message granting the match request
   */
  private Message createGrantingMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex, true);
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message denying the match request
   */
  private Message createDenyingMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex, false);
  }

}
