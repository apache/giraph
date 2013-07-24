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

import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import junit.framework.Assert;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Test ProgressableUtils
 */
public class TestProgressableUtils {
  @Test
  public void testProgressableUtils() throws NoSuchFieldException,
      IllegalAccessException {
    final int sleepTime = 1800;
    final int msecPeriod = 500;
    ExecutorService executor = Executors.newFixedThreadPool(1);
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw new IllegalStateException();
        }
      }
    });
    executor.shutdown();
    CountProgressable countProgressable = new CountProgressable();
    ProgressableUtils.awaitExecutorTermination(executor, countProgressable,
        msecPeriod);
    Assert.assertTrue(countProgressable.counter >= sleepTime / msecPeriod);
    Assert.assertTrue(
        countProgressable.counter <= (sleepTime + msecPeriod) / msecPeriod);
  }

  private static class CountProgressable implements Progressable {
    private int counter = 0;

    @Override
    public void progress() {
      counter++;
    }
  }
}
