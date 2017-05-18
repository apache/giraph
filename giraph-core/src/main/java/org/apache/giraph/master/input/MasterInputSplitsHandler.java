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
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handler for input splits on master
 *
 * Since currently Giraph fails if worker fails while reading input, we
 * didn't complicate this part with retries yet, later it could be added by
 * keeping track of which worker got which split and then if worker dies put
 * these splits back to queues.
 */
public class MasterInputSplitsHandler {
  /**
   * Store in counters timestamps when we finished reading
   * these fractions of input
   */
  public static final StrConfOption DONE_FRACTIONS_TO_STORE_IN_COUNTERS =
      new StrConfOption("giraph.master.input.doneFractionsToStoreInCounters",
          "0.99,1", "Store in counters timestamps when we finished reading " +
          "these fractions of input");

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
  /** Context for accessing counters */
  private final Mapper.Context context;
  /** How many splits per type are there total */
  private final Map<InputType, Integer> numSplitsPerType =
      new EnumMap<>(InputType.class);
  /** How many splits per type have been read so far */
  private final Map<InputType, AtomicInteger> numSplitsReadPerType =
      new EnumMap<>(InputType.class);
  /** Timestamps when various splits were created */
  private final Map<InputType, Long> splitsCreatedTimestamp =
      new EnumMap<>(InputType.class);
  /**
   * Store in counters timestamps when we finished reading
   * these fractions of input
   */
  private final double[] doneFractionsToStoreInCounters;

  /**
   * Constructor
   *
   * @param useLocality Whether to use locality information or not
   * @param context Context for accessing counters
   */
  public MasterInputSplitsHandler(boolean useLocality, Mapper.Context context) {
    this.useLocality = useLocality;
    this.context = context;
    for (InputType inputType : InputType.values()) {
      latchesMap.put(inputType, new CountDownLatch(1));
      numSplitsReadPerType.put(inputType, new AtomicInteger(0));
    }

    String[] tmp = DONE_FRACTIONS_TO_STORE_IN_COUNTERS.get(
        context.getConfiguration()).split(",");
    doneFractionsToStoreInCounters = new double[tmp.length];
    for (int i = 0; i < tmp.length; i++) {
      doneFractionsToStoreInCounters[i] = Double.parseDouble(tmp[i].trim());
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
    splitsCreatedTimestamp.put(splitsType, System.currentTimeMillis());
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
    numSplitsPerType.put(splitsType, serializedSplits.size());
  }

  /**
   * Called after we receive a split request from some worker, should send
   * split back to it if available, or send it information that there is no
   * more available
   *
   * @param splitType Type of split requested
   * @param workerTaskId Id of worker who requested split
   * @param isFirstSplit Whether this is the first split a thread is requesting,
   *   or this request indicates that previously requested input split was done
   */
  public void sendSplitTo(InputType splitType, int workerTaskId,
      boolean isFirstSplit) {
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
    if (!isFirstSplit) {
      incrementSplitsRead(splitType);
    }
  }

  /**
   * Increment splits read
   *
   * @param splitType Type of split which was read
   */
  private void incrementSplitsRead(InputType splitType) {
    int splitsRead = numSplitsReadPerType.get(splitType).incrementAndGet();
    int splits = numSplitsPerType.get(splitType);
    for (int i = 0; i < doneFractionsToStoreInCounters.length; i++) {
      if (splitsRead == (int) (splits * doneFractionsToStoreInCounters[i])) {
        splitFractionReached(
            splitType, doneFractionsToStoreInCounters[i], context);
      }
    }
  }

  /**
   * Call when we reached some fraction of split type done to set the
   * timestamp counter
   *
   * @param inputType Type of input
   * @param fraction Which fraction of input type was done reading
   * @param context Context for accessing counters
   */
  private void splitFractionReached(
      InputType inputType, double fraction, Mapper.Context context) {
    getSplitFractionDoneTimestampCounter(inputType, fraction, context).setValue(
        System.currentTimeMillis() - splitsCreatedTimestamp.get(inputType));
  }

  /**
   * Get counter
   *
   * @param inputType Type of input for counter
   * @param fraction Fraction for counter
   * @param context Context to get counter from
   * @return Counter
   */
  public static Counter getSplitFractionDoneTimestampCounter(
      InputType inputType, double fraction, Mapper.Context context) {
    return context.getCounter(inputType.name() + " input",
        String.format("%.2f%% done time (ms)", fraction * 100));
  }
}
