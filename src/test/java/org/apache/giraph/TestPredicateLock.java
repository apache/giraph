/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import junit.framework.TestCase;

import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;

/**
 * Ensure that PredicateLock objects work correctly.
 */
public class TestPredicateLock extends TestCase {
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

    /**
     * Make sure the the event is not signaled.
     */
    public void testWaitMsecsNoEvent() {
        BspEvent event = new PredicateLock();
        boolean gotPredicate = event.waitMsecs(50);
        assertTrue(gotPredicate == false);
    }

    /**
     * Single threaded case
     */
    public void testEvent() {
        BspEvent event = new PredicateLock();
        event.signal();
        boolean gotPredicate = event.waitMsecs(-1);
        assertTrue(gotPredicate == true);
        event.reset();
        gotPredicate = event.waitMsecs(0);
        assertTrue(gotPredicate == false);
    }

    /**
     * Make sure the the event is signaled correctly
     * @throws InterruptedException
     */
    public void testWaitMsecs() {
        System.out.println("testWaitMsecs:");
        BspEvent event = new PredicateLock();
        Thread signalThread = new SignalThread(event);
        signalThread.start();
        boolean gotPredicate = event.waitMsecs(2000);
        assertTrue(gotPredicate == true);
        try {
            signalThread.join();
        } catch (InterruptedException e) {
        }
        gotPredicate = event.waitMsecs(0);
        assertTrue(gotPredicate == true);
    }
}
