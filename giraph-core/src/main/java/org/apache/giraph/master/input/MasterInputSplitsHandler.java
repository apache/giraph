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

package org.apache.giraph.master.input;

import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.requests.ReplyWithInputSplitRequest;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Handler for input splits on master
 *
 * Since currently Giraph fails if worker fails while reading input, we
 * didn't complicate this part with retries yet, later it could be added by
 * keeping track of which worker got which split and then if worker dies put
 * these splits back to queues.
 */
public class MasterInputSplitsHandler {
  /** Whether to use locality information */
  private final boolean useLocality;
  /** Master client */
  private MasterClient masterClient;
  /** Master client */
  private List<WorkerInfo> workers;
  /** Map of splits organizers for each split type */
  private Map<InputType, InputSplitsMasterOrganizer> splitsMap =
      new EnumMap<>(InputType.class);
  /** Latches to say when one input splits type is ready to be accessed */
  private Map<InputType, CountDownLatch> latchesMap =
      new EnumMap<>(InputType.class);

  /**
   * Constructor
   *
   * @param useLocality Whether to use locality information or not
   */
  public MasterInputSplitsHandler(boolean useLocality) {
    this.useLocality = useLocality;
    for (InputType inputType : InputType.values()) {
      latchesMap.put(inputType, new CountDownLatch(1));
    }
  }

  /**
   * Initialize
   *
   * @param masterClient Master client
   * @param workers List of workers
   */
  public void initialize(MasterClient masterClient, List<WorkerInfo> workers) {
    this.masterClient = masterClient;
    this.workers = workers;
  }

  /**
   * Add splits
   *
   * @param splitsType Type of splits
   * @param inputSplits Splits
   * @param inputFormat Format
   */
  public void addSplits(InputType splitsType, List<InputSplit> inputSplits,
      GiraphInputFormat inputFormat) {
    List<byte[]> serializedSplits = new ArrayList<>();
    for (InputSplit inputSplit : inputSplits) {
      try {
        ByteArrayOutputStream byteArrayOutputStream =
            new ByteArrayOutputStream();
        DataOutput outputStream =
            new DataOutputStream(byteArrayOutputStream);
        inputFormat.writeInputSplit(inputSplit, outputStream);
        serializedSplits.add(byteArrayOutputStream.toByteArray());
      } catch (IOException e) {
        throw new IllegalStateException("IOException occurred", e);
      }
    }
    InputSplitsMasterOrganizer inputSplitsOrganizer;
    if (splitsType == InputType.MAPPING) {
      inputSplitsOrganizer = new MappingInputSplitsMasterOrganizer(
          serializedSplits, workers);
    } else {
      inputSplitsOrganizer = useLocality ?
          new LocalityAwareInputSplitsMasterOrganizer(serializedSplits,
              inputSplits, workers) :
          new BasicInputSplitsMasterOrganizer(serializedSplits);
    }
    splitsMap.put(splitsType, inputSplitsOrganizer);
    latchesMap.get(splitsType).countDown();
  }

  /**
   * Called after we receive a split request from some worker, should send
   * split back to it if available, or send it information that there is no
   * more available
   *
   * @param splitType Type of split requested
   * @param workerTaskId Id of worker who requested split
   */
  public void sendSplitTo(InputType splitType, int workerTaskId) {
    try {
      // Make sure we don't try to retrieve splits before they were added
      latchesMap.get(splitType).await();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted", e);
    }
    byte[] serializedInputSplit =
        splitsMap.get(splitType).getSerializedSplitFor(workerTaskId);
    masterClient.sendWritableRequest(workerTaskId,
        new ReplyWithInputSplitRequest(splitType,
            serializedInputSplit == null ? new byte[0] : serializedInputSplit));
  }
}
