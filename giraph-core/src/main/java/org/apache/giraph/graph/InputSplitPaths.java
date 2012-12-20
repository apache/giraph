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

package org.apache.giraph.graph;

/**
 * Simple container of input split paths for coordination via ZooKeeper.
 */
public class InputSplitPaths {
  /** Path to the input splits written by the master */
  private final String path;
  /** Path to the input splits all ready to be processed by workers */
  private final String allReadyPath;
  /** Path to the input splits done */
  private final String donePath;
  /** Path to the input splits all done to notify the workers to proceed */
  private final String allDonePath;

  /**
   * Constructor.
   *
   * @param basePath Base path
   * @param dir Input splits path
   * @param doneDir Input split done path
   * @param allReadyNode Input splits all ready path
   * @param allDoneNode Input splits all done path
   */
  public InputSplitPaths(String basePath,
                         String dir,
                         String doneDir,
                         String allReadyNode,
                         String allDoneNode) {
    path = basePath + dir;
    allReadyPath = basePath + allReadyNode;
    donePath = basePath + doneDir;
    allDonePath = basePath + allDoneNode;
  }

  /**
   * Get path to the input splits.
   *
   * @return Path to input splits
   */
  public String getPath() {
    return path;
  }

  /**
   * Get path to the input splits all ready.
   *
   * @return Path to input splits all ready
   */
  public String getAllReadyPath() {
    return allReadyPath;
  }

  /** Get path to the input splits done.
   *
   * @return Path to input splits done
   */
  public String getDonePath() {
    return donePath;
  }

  /**
   * Get path to the input splits all done.
   *
   * @return Path to input splits all done
   */
  public String getAllDonePath() {
    return allDonePath;
  }
}
