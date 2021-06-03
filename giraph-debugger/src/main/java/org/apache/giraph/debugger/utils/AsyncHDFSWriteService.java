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
package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.protobuf.GeneratedMessage;

/**
 * A utility class for writing to HDFS asynchronously.
 */
public class AsyncHDFSWriteService {

  /**
   * Logger for this class.
   */
  protected static final Logger LOG = Logger
    .getLogger(AsyncHDFSWriteService.class);

  /**
   * The thread pool that will handle the synchronous writing, and hide the
   * latency from the callers.
   */
  private static ExecutorService HDFS_ASYNC_WRITE_SERVICE = Executors
    .newFixedThreadPool(2);
  static {
    // Make sure we finish writing everything before shuting down the VM.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.info("Shutting down writer");
        HDFS_ASYNC_WRITE_SERVICE.shutdown();
        LOG.info("Waiting until finishes all writes");
        try {
          HDFS_ASYNC_WRITE_SERVICE.awaitTermination(Long.MAX_VALUE,
            TimeUnit.NANOSECONDS);
          LOG.info("Finished all writes");
        } catch (InterruptedException e) {
          LOG.error("Could not finish all writes");
          e.printStackTrace();
        }
      }
    }));
  }

  /**
   * Not for instantiation.
   */
  private AsyncHDFSWriteService() {
  }

  /**
   * Writes given protobuf message to the given filesystem path in the
   * background.
   *
   * @param message
   *          The proto message to write.
   * @param fs
   *          The HDFS filesystem to write to.
   * @param fileName
   *          The HDFS path to write the message to.
   */
  public static void writeToHDFS(final GeneratedMessage message,
    final FileSystem fs, final String fileName) {
    HDFS_ASYNC_WRITE_SERVICE.submit(new Runnable() {
      @Override
      public void run() {
        Path pt = new Path(fileName);
        try {
          LOG.info("Writing " + fileName + " at " + fs.getUri());
          OutputStream wrappedStream = fs.create(pt, true).getWrappedStream();
          message.writeTo(wrappedStream);
          wrappedStream.close();
          LOG.info("Done writing " + fileName);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
  }

}
