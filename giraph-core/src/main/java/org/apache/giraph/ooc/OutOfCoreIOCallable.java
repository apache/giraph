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

import org.apache.giraph.ooc.io.IOCommand;
import org.apache.giraph.ooc.io.WaitIOCommand;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * IO threads for out-of-core mechanism.
 */
public class OutOfCoreIOCallable implements Callable<Void> {
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(OutOfCoreIOCallable.class);
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Base path that this thread will write to/read from */
  private final String basePath;
  /** Thread id/Disk id */
  private final int diskId;

  /**
   * Constructor
   *
   * @param oocEngine out-of-core engine
   * @param basePath base path this thread will be using
   * @param diskId thread id/disk id
   */
  public OutOfCoreIOCallable(OutOfCoreEngine oocEngine, String basePath,
                             int diskId) {
    this.oocEngine = oocEngine;
    this.basePath = basePath;
    this.diskId = diskId;
  }

  @Override
  public Void call() {
    while (true) {
      oocEngine.getSuperstepLock().readLock().lock();
      IOCommand command = oocEngine.getIOScheduler().getNextIOCommand(diskId);
      if (LOG.isInfoEnabled()) {
        LOG.info("call: thread " + diskId + "'s next IO command is: " +
            command);
      }
      if (command == null) {
        oocEngine.getSuperstepLock().readLock().unlock();
        break;
      }
      if (command instanceof WaitIOCommand) {
        oocEngine.getSuperstepLock().readLock().unlock();
      }
      // CHECKSTYLE: stop IllegalCatch
      try {
        command.execute(basePath);
      } catch (Exception e) {
        oocEngine.failTheJob();
        LOG.info("call: execution of IO command " + command + " failed!");
        throw new RuntimeException(e);
      }
      // CHECKSTYLE: resume IllegalCatch
      if (!(command instanceof WaitIOCommand)) {
        oocEngine.getSuperstepLock().readLock().unlock();
      }
      oocEngine.getIOScheduler().ioCommandCompleted(command);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("call: out-of-core IO thread " + diskId + " terminating!");
    }
    return null;
  }
}

