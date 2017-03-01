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

package org.apache.giraph.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.giraph.time.Time;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensure that PredicateLock objects work correctly.
 */
public class TestPredicateLock {
  /** How many times was progress called? */
  private AtomicInteger progressCalled = new AtomicInteger(0);

  private static class SignalThread extends Thread {
    private final BspEvent event;
    public SignalThread(BspEvent event) {
      this.event = event;
    }
    public void run() {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
      event.signal();
    }
  }

  private Progressable stubContext;

  private Progressable getStubProgressable() {
    if (stubContext == null)
      stubContext = new Progressable() {
        @Override
        public void progress() {
          progressCalled.incrementAndGet();
        }
      };
    return stubContext;
  }

  @Before
  public void setUp() {
    progressCalled.set(0);
  }

  /**
   * SMake sure the the event is not signaled.
   */
  @Test
  public void testWaitMsecsNoEvent() {
    Time mockTime = mock(Time.class);
    when(mockTime.getMilliseconds()).
        thenReturn(0L).thenReturn(2L);
    BspEvent event = new PredicateLock(getStubProgressable(), 1, mockTime);
    boolean gotPredicate = event.waitMsecs(1);
    assertFalse(gotPredicate);
    assertEquals(0, progressCalled.get());
    when(mockTime.getMilliseconds()).
        thenReturn(0L).thenReturn(0L).thenReturn(2L);
    gotPredicate = event.waitMsecs(1);
    assertFalse(gotPredicate);
    assertEquals(1, progressCalled.get());
  }

  /**
   * Single threaded case where the event is signaled.
   */
  @Test
  public void testEvent() {
    Time mockTime = mock(Time.class);
    when(mockTime.getMilliseconds()).
        thenReturn(0L).thenReturn(2L);
    BspEvent event = new PredicateLock(getStubProgressable(), 1, mockTime);
    event.signal();
    boolean gotPredicate = event.waitMsecs(2);
    assertTrue(gotPredicate);
    event.reset();
    when(mockTime.getMilliseconds()).
        thenReturn(0L).thenReturn(2L);
    gotPredicate = event.waitMsecs(0);
    assertFalse(gotPredicate);
  }

  /**
   * Thread signaled test for {@link PredicateLock#waitForTimeoutOrFail(long)}
   */
  @Test
  public void testWaitForever() {
    BspEvent event = new PredicateLock(getStubProgressable());
    Thread signalThread = new SignalThread(event);
    signalThread.start();
    event.waitForTimeoutOrFail(5 * 60_000);
    try {
      signalThread.join();
    } catch (InterruptedException e) {
    }
    assertTrue(event.waitMsecs(0));
  }

  /**
   * Thread signaled test to make sure the the event is signaled correctly
   */
  @Test
  public void testWaitMsecs() {
    BspEvent event = new PredicateLock(getStubProgressable());
    Thread signalThread = new SignalThread(event);
    signalThread.start();
    boolean gotPredicate = event.waitMsecs(2000);
    assertTrue(gotPredicate);
    try {
      signalThread.join();
    } catch (InterruptedException e) {
    }
    gotPredicate = event.waitMsecs(0);
    assertTrue(gotPredicate);
  }
}
