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

package org.apache.giraph.zk;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * A lock that will keep the job context updated while waiting.
 */
public class ContextLock extends PredicateLock {
    /** Job context (for progress) */
    @SuppressWarnings("rawtypes")
    private final Context context;
    /** Msecs to refresh the progress meter */
    private static final int msecPeriod = 10000;

    /**
     * Constructor.
     *
     * @param context used to call progress()
     */
    ContextLock(@SuppressWarnings("rawtypes") Context context) {
        this.context = context;
    }

    /**
     * Specialized version of waitForever() that will keep the job progressing
     * while waiting.
     */
    @Override
    public void waitForever() {
        while (waitMsecs(msecPeriod) == false) {
            context.progress();
        }
    }
}
