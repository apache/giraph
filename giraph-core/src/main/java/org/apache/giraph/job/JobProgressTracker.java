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

package org.apache.giraph.job;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import org.apache.giraph.master.MasterProgress;
import org.apache.giraph.worker.WorkerProgress;

/**
 * Interface for job progress tracker on job client
 */
@ThriftService
public interface JobProgressTracker {

  /** Notify JobProgressTracker that mapper started */
  @ThriftMethod
  void mapperStarted();

  /**
   * Call this when you want to log an info line from any mapper to command line
   *
   * @param logLine Line to log
   */
  @ThriftMethod
  void logInfo(String logLine);

  /**
   * Call this when you want to log an error line and exception
   * object from any mapper to command line
   *
   * KryoWritableWrapper.convertFromByteArray can be used to
   * get exception object back
   *
   * @param logLine Line to log
   * @param exByteArray Exception byte array
   */
  @ThriftMethod
  void logError(String logLine, byte [] exByteArray);

  /**
   * Notify that job is failing
   *
   * @param reason Reason for failure
   */
  @ThriftMethod
  void logFailure(String reason);

  /**
   * Workers should call this method to update their progress
   *
   * @param workerProgress Progress of the worker
   */
  @ThriftMethod
  void updateProgress(WorkerProgress workerProgress);

  /**
   * Master should call this method to update its progress
   *
   * @param masterProgress Progress of the master
   */
  @ThriftMethod
  void updateMasterProgress(MasterProgress masterProgress);
}

