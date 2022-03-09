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

package org.apache.giraph.ooc.command;

import org.apache.giraph.ooc.OutOfCoreEngine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * IOCommand to do nothing regarding moving data to/from disk.
 */
public class WaitIOCommand extends IOCommand {
  /** How long should the disk be idle? (in milliseconds) */
  private final long waitDuration;

  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param waitDuration duration of wait
   */
  public WaitIOCommand(OutOfCoreEngine oocEngine, long waitDuration) {
    super(oocEngine, -1);
    this.waitDuration = waitDuration;
  }

  @Override
  public boolean execute() throws IOException {
    try {
      TimeUnit.MILLISECONDS.sleep(waitDuration);
    } catch (InterruptedException e) {
      throw new IllegalStateException("execute: caught InterruptedException " +
          "while IO thread is waiting!");
    }
    return true;
  }

  @Override
  public IOCommandType getType() {
    return IOCommandType.WAIT;
  }

  @Override
  public String toString() {
    return "WaitIOCommand: (duration = " + waitDuration + "ms)";
  }
}
