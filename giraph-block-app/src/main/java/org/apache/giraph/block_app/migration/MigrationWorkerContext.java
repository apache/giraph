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
package org.apache.giraph.block_app.migration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockWorkerContextApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Replacement for WorkerContext when migrating to Blocks Framework,
 * disallowing functions that are tied to execution order.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MigrationWorkerContext
    extends DefaultImmutableClassesGiraphConfigurable
    implements Writable {
  private BlockWorkerContextApi api;
  private List<Writable> receivedMessages;

  public void setApi(BlockWorkerContextApi api) {
    this.api = api;
    this.setConf(api.getConf());
  }

  public void setReceivedMessages(List<Writable> receivedMessages) {
    this.receivedMessages = receivedMessages;
  }

  public void preSuperstep() { }

  public void postSuperstep() { }

  @SuppressWarnings("deprecation")
  public long getTotalNumVertices() {
    return api.getTotalNumVertices();
  }

  @SuppressWarnings("deprecation")
  public long getTotalNumEdges() {
    return api.getTotalNumEdges();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  public final int getWorkerCount() {
    return api.getWorkerCount();
  }

  public final int getMyWorkerIndex() {
    return api.getMyWorkerIndex();
  }

  public final List<Writable> getAndClearMessagesFromOtherWorkers() {
    List<Writable> ret = receivedMessages;
    receivedMessages = null;
    return ret;
  }

  public final void sendMessageToWorker(Writable message, int workerIndex) {
    ((BlockWorkerContextSendApi<WritableComparable, Writable>) api)
      .sendMessageToWorker(message, workerIndex);
  }

  /**
   * Drop-in replacement for WorkerContext when migrating to
   * Blocks Framework.
   */
  public static class MigrationFullWorkerContext
      extends MigrationWorkerContext {
    public void preApplication()
        throws InstantiationException, IllegalAccessException {
    }

    public void postApplication() { }
  }
}
