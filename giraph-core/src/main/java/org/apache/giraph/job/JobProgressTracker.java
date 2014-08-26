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

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.worker.WorkerProgress;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

/**
 * Interface for job progress tracker on job client
 */
@ThriftService
public interface JobProgressTracker {
  /** Host on which job progress service runs */
  StrConfOption JOB_PROGRESS_SERVICE_HOST =
      new StrConfOption("giraph.jobProgressServiceHost", null,
          "Host on which job progress service runs");
  /** Port which job progress service uses */
  IntConfOption JOB_PROGRESS_SERVICE_PORT =
      new IntConfOption("giraph.jobProgressServicePort", -1,
          "Port which job progress service uses");

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
}

