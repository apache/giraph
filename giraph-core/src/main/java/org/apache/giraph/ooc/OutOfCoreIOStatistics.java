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

package org.apache.giraph.ooc;

import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.ooc.command.IOCommand.IOCommandType;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to collect statistics regarding IO operations done in out-of-core
 * mechanism.
 */
public class OutOfCoreIOStatistics {
  /**
   * An estimate of disk bandwidth. This number is only used just at the start
   * of the computation, and will be updated/replaced later on once a few disk
   * operations happen.
   */
  public static final IntConfOption DISK_BANDWIDTH_ESTIMATE =
      new IntConfOption("giraph.diskBandwidthEstimate", 125,
          "An estimate of disk bandwidth (MB/s). This number is used just at " +
              "the beginning of the computation, and it will be " +
              "updated/replaced once a few disk operations happen.");
  /**
   * How many recent IO operations should we keep track of? Any report given out
   * of this statistics collector is only based on most recent IO operations.
   */
  public static final IntConfOption IO_COMMAND_HISTORY_SIZE =
      new IntConfOption("giraph.ioCommandHistorySize", 50,
          "Number of most recent IO operations to consider for reporting the" +
              "statistics.");

  /**
   * Use this option to control how frequently to print OOC statistics.
   */
  public static final IntConfOption STATS_PRINT_FREQUENCY =
      new IntConfOption("giraph.oocStatPrintFrequency", 200,
          "Number of updates before stats are printed.");

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(OutOfCoreIOStatistics.class);
  /** Estimate of disk bandwidth (bytes/second) */
  private final AtomicLong diskBandwidthEstimate;
  /** Cached value for IO_COMMAND_HISTORY_SIZE */
  private final int maxHistorySize;
  /**
   * Coefficient/Weight of the most recent IO operation toward the disk
   * bandwidth estimate. Basically if the disk bandwidth estimate if d, and the
   * latest IO command happened at the rate of r, the new estimate of disk
   * bandwidth is calculated as:
   * d_new = updateCoefficient * r + (1 - updateCoefficient) * d
   */
  private final double updateCoefficient;
  /** Queue of all recent commands */
  private final Queue<StatisticsEntry> commandHistory;
  /**
   * Command statistics for each type of IO command. This is the statistics of
   * the recent commands in the history we keep track of (with 'maxHistorySize'
   * command in the history).
   */
  private final Map<IOCommandType, StatisticsEntry> aggregateStats;
  /** How many IO command completed? */
  private int numUpdates = 0;
  /** Cached value for {@link #STATS_PRINT_FREQUENCY} */
  private int statsPrintFrequency = 0;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param numIOThreads number of disks/IO threads
   */
  public OutOfCoreIOStatistics(ImmutableClassesGiraphConfiguration conf,
                               int numIOThreads) {
    this.diskBandwidthEstimate =
        new AtomicLong(DISK_BANDWIDTH_ESTIMATE.get(conf) *
            (long) GiraphConstants.ONE_MB);
    this.maxHistorySize = IO_COMMAND_HISTORY_SIZE.get(conf);
    this.updateCoefficient = 1.0 / maxHistorySize;
    // Adding more entry to the capacity of the queue to have a wiggle room
    // if all IO threads are adding/removing entries from the queue
    this.commandHistory =
        new ArrayBlockingQueue<>(maxHistorySize + numIOThreads);
    this.aggregateStats = Maps.newConcurrentMap();
    for (IOCommandType type : IOCommandType.values()) {
      aggregateStats.put(type, new StatisticsEntry(type, 0, 0, 0));
    }
    this.statsPrintFrequency = STATS_PRINT_FREQUENCY.get(conf);
  }

  /**
   * Update statistics with the last IO command that is executed.
   *
   * @param type type of the IO command that is executed
   * @param bytesTransferred number of bytes transferred in the last IO command
   * @param duration duration it took for the last IO command to complete
   *                 (milliseconds)
   */
  public void update(IOCommandType type, long bytesTransferred,
                     long duration) {
    StatisticsEntry entry = aggregateStats.get(type);
    synchronized (entry) {
      entry.setOccurrence(entry.getOccurrence() + 1);
      entry.setDuration(duration + entry.getDuration());
      entry.setBytesTransferred(bytesTransferred + entry.getBytesTransferred());
    }
    commandHistory.offer(
        new StatisticsEntry(type, bytesTransferred, duration, 0));
    if (type != IOCommandType.WAIT) {
      // If the current estimate is 'd', the new rate is 'r', and the size of
      // history is 'n', we can simply model all the past command's rate as:
      // d, d, d, ..., d, r
      // where 'd' happens for 'n-1' times. Hence the new estimate of the
      // bandwidth would be:
      // d_new = (d * (n-1) + r) / n = (1-1/n)*d + 1/n*r
      // where updateCoefficient = 1/n
      diskBandwidthEstimate.set((long)
          (updateCoefficient * (bytesTransferred / duration * 1000) +
              (1 - updateCoefficient) * diskBandwidthEstimate.get()));
    }
    if (commandHistory.size() > maxHistorySize) {
      StatisticsEntry removedEntry = commandHistory.poll();
      entry = aggregateStats.get(removedEntry.getType());
      synchronized (entry) {
        entry.setOccurrence(entry.getOccurrence() - 1);
        entry.setDuration(entry.getDuration() - removedEntry.getDuration());
        entry.setBytesTransferred(
            entry.getBytesTransferred() - removedEntry.getBytesTransferred());
      }
    }
    numUpdates++;
    // Outputting log every so many commands
    if (numUpdates % statsPrintFrequency == 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info(this);
      }
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    long waitTime = 0;
    long loadTime = 0;
    long bytesRead = 0;
    long storeTime = 0;
    long bytesWritten = 0;
    for (Map.Entry<IOCommandType, StatisticsEntry> entry :
        aggregateStats.entrySet()) {
      synchronized (entry.getValue()) {
        sb.append(entry.getKey() + ": " + entry.getValue() + ", ");
        if (entry.getKey() == IOCommandType.WAIT) {
          waitTime += entry.getValue().getDuration();
        } else if (entry.getKey() == IOCommandType.LOAD_PARTITION) {
          loadTime += entry.getValue().getDuration();
          bytesRead += entry.getValue().getBytesTransferred();
        } else {
          storeTime += entry.getValue().getDuration();
          bytesWritten += entry.getValue().getBytesTransferred();
        }
      }
    }
    sb.append(String.format("Average STORE: %.2f MB/s, ",
        (double) bytesWritten / storeTime * 1000 / 1024 / 1024));
    sb.append(String.format("DATA_INJECTION: %.2f MB/s, ",
        (double) (bytesRead - bytesWritten) /
            (waitTime + loadTime + storeTime) * 1000 / 1024 / 1024));
    sb.append(String.format("DISK_BANDWIDTH: %.2f MB/s",
        (double) diskBandwidthEstimate.get() / 1024 / 1024));

    return sb.toString();
  }

  /**
   * @return most recent estimate of the disk bandwidth
   */
  public long getDiskBandwidth() {
    return diskBandwidthEstimate.get();
  }

  /**
   * Get aggregate statistics of a given command type in the command history
   *
   * @param type type of the command to get the statistics for
   * @return aggregate statistics for the given command type
   */
  public BytesDuration getCommandTypeStats(IOCommandType type) {
    StatisticsEntry entry = aggregateStats.get(type);
    synchronized (entry) {
      return new BytesDuration(entry.getBytesTransferred(), entry.getDuration(),
          entry.getOccurrence());
    }
  }

  /**
   * Helper class to return results of statistics collector for a certain
   * command type
   */
  public static class BytesDuration {
    /** Number of bytes transferred in a few commands of the same type */
    private long bytes;
    /** Duration of it took to execute a few commands of the same type */
    private long duration;
    /** Number of commands executed of the same type */
    private int occurrence;

    /**
     * Constructor
     * @param bytes number of bytes transferred
     * @param duration duration it took to execute commands
     * @param occurrence number of commands
     */
    BytesDuration(long bytes, long duration, int occurrence) {
      this.bytes = bytes;
      this.duration = duration;
      this.occurrence = occurrence;
    }

    /**
     * @return number of bytes transferred for the same command type
     */
    public long getBytes() {
      return bytes;
    }

    /**
     * @return duration it took to execute a few commands of the same type
     */
    public long getDuration() {
      return duration;
    }

    /**
     * @return number of commands that are executed of the same type
     */
    public int getOccurrence() {
      return occurrence;
    }
  }

  /**
   * Helper class to keep statistics for a certain command type
   */
  private static class StatisticsEntry {
    /** Type of the command */
    private IOCommandType type;
    /**
     * Aggregate number of bytes transferred executing the particular command
     * type in the history we keep
     */
    private long bytesTransferred;
    /**
     * Aggregate duration it took executing the particular command type in the
     * history we keep
     */
    private long duration;
    /**
     * Number of occurrences of the particular command type in the history we
     * keep
     */
    private int occurrence;

    /**
     * Constructor
     *
     * @param type type of the command
     * @param bytesTransferred aggregate number of bytes transferred
     * @param duration aggregate execution time
     * @param occurrence number of occurrences of the particular command type
     */
    public StatisticsEntry(IOCommandType type, long bytesTransferred,
                           long duration, int occurrence) {
      this.type = type;
      this.bytesTransferred = bytesTransferred;
      this.duration = duration;
      this.occurrence = occurrence;
    }

    /**
     * @return type of the command
     */
    public IOCommandType getType() {
      return type;
    }

    /**
     * @return aggregate number of bytes transferred in the particular command
     *         type
     */
    public long getBytesTransferred() {
      return bytesTransferred;
    }

    /**
     * Update the aggregate number of bytes transferred
     *
     * @param bytesTransferred aggregate number of bytes
     */
    public void setBytesTransferred(long bytesTransferred) {
      this.bytesTransferred = bytesTransferred;
    }

    /**
     * @return aggregate duration it took to execute the particular command type
     */
    public long getDuration() {
      return duration;
    }

    /**
     * Update the aggregate duration
     *
     * @param duration aggregate duration
     */
    public void setDuration(long duration) {
      this.duration = duration;
    }

    /**
     * @return number of occurrences of the particular command type
     */
    public int getOccurrence() {
      return occurrence;
    }

    /**
     * Update the number of occurrences of the particular command type
     *
     * @param occurrence number of occurrences
     */
    public void setOccurrence(int occurrence) {
      this.occurrence = occurrence;
    }

    @Override
    public String toString() {
      if (type == IOCommandType.WAIT) {
        return String.format("%.2f sec", duration / 1000.0);
      } else {
        return String.format("%.2f MB/s",
            (double) bytesTransferred / duration * 1000 / 1024 / 2014);
      }
    }
  }
}
