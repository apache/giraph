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

package org.apache.giraph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

/**
 * Ensure that PredicateLock objects work correctly.
 */
public class TestPredicateLock {
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
                        System.out.println("progress received");
                    }
            };
        return stubContext;
    }

    /**
     * Make sure the the event is not signaled.
     */
  @Test
    public void testWaitMsecsNoEvent() {
        BspEvent event = new PredicateLock(getStubProgressable());
        boolean gotPredicate = event.waitMsecs(50);
        assertFalse(gotPredicate);
    }

    /**
     * Single threaded case
     */
  @Test
    public void testEvent() {
        BspEvent event = new PredicateLock(getStubProgressable());
        event.signal();
        boolean gotPredicate = event.waitMsecs(50);
        assertTrue(gotPredicate);
        event.reset();
        gotPredicate = event.waitMsecs(0);
        assertFalse(gotPredicate);
    }

  /**
   * Simple test for {@link PredicateLock#waitForever()}
   */
  @Test
  public void testWaitForever() {
    BspEvent event = new PredicateLock(getStubProgressable());
    Thread signalThread = new SignalThread(event);
    signalThread.start();
    event.waitForever();
    try {
      signalThread.join();
    } catch (InterruptedException e) {
    }
    assertTrue(event.waitMsecs(0));
  }

    /**
     * Make sure the the event is signaled correctly
     * @throws InterruptedException
     */
  @Test
    public void testWaitMsecs() {
        System.out.println("testWaitMsecs:");
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
