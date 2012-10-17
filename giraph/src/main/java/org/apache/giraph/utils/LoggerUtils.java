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

import java.io.IOException;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Logger utils for log4j
 */
public class LoggerUtils {
  /**
   * Don't construct this.
   */
  private LoggerUtils() { }

  /**
   * Helper method to set the status and log message together.
   *
   * @param context Context to set the status with
   * @param logger Logger to write to
   * @param level Level of logging
   * @param message Message to
   */
  public static void setStatusAndLog(
      TaskAttemptContext context, Logger logger, Level level,
      String message) {
    try {
      context.setStatus(message);
    } catch (IOException e) {
      throw new IllegalStateException("setStatusAndLog: Got IOException", e);
    }
    if (logger.isEnabledFor(level)) {
      logger.log(level, message);
    }
  }
}
