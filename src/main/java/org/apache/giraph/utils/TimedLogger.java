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

package org.apache.giraph.utils;

import org.apache.log4j.Logger;

/**
 * Print log messages only if the time is met.  Thread-safe.
 */
public class TimedLogger {
  /** Last time printed */
  private volatile long lastPrint = System.currentTimeMillis();
  /** Minimum interval of time to wait before printing */
  private final int msecs;
  /** Logger */
  private final Logger log;

  /**
   * Constructor of the timed logger
   *
   * @param msecs Msecs to wait before printing again
   * @param log Logger to print to
   */
  public TimedLogger(int msecs, Logger log) {
    this.msecs = msecs;
    this.log = log;
  }

  /**
   * Print to the info log level if the minimum waiting time was reached.
   *
   * @param msg Message to print
   */
  public void info(String msg) {
    if (isPrintable()) {
      log.info(msg);
    }
  }

  /**
   * Is the log message printable (minimum interval met)?
   *
   * @return True if the message is printable
   */
  public boolean isPrintable() {
    if (System.currentTimeMillis() > lastPrint + msecs) {
      lastPrint = System.currentTimeMillis();
      return true;
    }

    return false;
  }
}
