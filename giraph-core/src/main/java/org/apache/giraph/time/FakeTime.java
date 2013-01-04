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

package org.apache.giraph.time;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe implementation of Time for testing that can help get time based
 * ordering of events when desired.
 */
public class FakeTime implements Time {
  /** Nanoseconds from the fake epoch */
  private final AtomicLong nanosecondsSinceEpoch = new AtomicLong();

  @Override
  public long getMilliseconds() {
    return nanosecondsSinceEpoch.get() / NS_PER_MS;
  }

  @Override
  public long getMicroseconds() {
    return nanosecondsSinceEpoch.get() / NS_PER_US;
  }

  @Override
  public long getNanoseconds() {
    return nanosecondsSinceEpoch.get();
  }

  @Override
  public int getSeconds() {
    return (int) (nanosecondsSinceEpoch.get() / NS_PER_SECOND);
  }

  @Override
  public Date getCurrentDate() {
    return new Date(getMilliseconds());
  }

  @Override
  public void sleep(long milliseconds) throws InterruptedException {
    nanosecondsSinceEpoch.getAndAdd(milliseconds * NS_PER_MS);
  }
}
