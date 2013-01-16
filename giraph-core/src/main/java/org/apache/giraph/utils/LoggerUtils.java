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
   * Helper method to set the status and log message together if condition
   * has been been met.
   *
   * @param condition Must be true to write status and log
   * @param context Context to set the status with
   * @param logger Logger to write to
   * @param level Level of logging
   * @param message Message to set status with
   */
  public static void conditionalSetStatusAndLog(
      boolean condition,
      TaskAttemptContext context, Logger logger, Level level,
      String message) {
    if (condition) {
      setStatusAndLog(context, logger, level, message);
    }
  }

  /**
   * Helper method to set the status and log message together.
   *
   * @param context Context to set the status with
   * @param logger Logger to write to
   * @param level Level of logging
   * @param message Message to set status with
   */
  public static void setStatusAndLog(
      TaskAttemptContext context, Logger logger, Level level,
      String message) {
    try {
      setStatus(context, message);
    } catch (IOException e) {
      throw new IllegalStateException("setStatusAndLog: Got IOException", e);
    }
    if (logger.isEnabledFor(level)) {
      logger.log(level, message);
    }
  }

  /**
   * Set Hadoop status message.
   *
   * NOTE: In theory this function could get folded in to the callsites, but
   * the issue is that some Hadoop jars, e.g. 0.23 and 2.0.0, don't actually
   * throw IOException on setStatus while others do. This makes wrapping it in a
   * try/catch cause a compile error on those Hadoops. With this function every
   * caller sees a method that throws IOException. In case it doesn't actually,
   * there is no more compiler error because not throwing a decalred exception
   * is at best a warning.
   *
   * @param context Context to set the status with
   * @param message Message to set status with
   * @throws IOException If something goes wrong with setting status message
   */
  private static void setStatus(TaskAttemptContext context, String message)
    throws IOException {
    context.setStatus(message);
  }
}
