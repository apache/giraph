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
package org.apache.giraph.writable.kryo;

import java.util.Random;

/**
 * Transient Random class. Seed/state is not kept after
 * serializing/deserializing.
 *
 * Within Blocks Framework - if we initialize Random within the Piece, when
 * it's serialzied and copied to all workers and all threads - keeping seed
 * would cause same series of random numbers to be generated everywhere.
 *
 * So this class is safe to be used in Pieces, while using regular Random
 * class is forbidden to be serialized.
 * Best approach would be to not have Random serialized, and create it on
 * workers, where possible.
 */
public class TransientRandom {
  /** Instance of random object */
  private final transient Random random = new Random();

  /**
   * Get instance of Random
   * @return Random instance
   */
  public Random get() {
    return random;
  }

  /**
   * Returns a pseudorandom, uniformly distributed {@code int} value
   * between 0 (inclusive) and the specified value (exclusive), drawn from
   * this random number generator's sequence.
   *
   * @param n Given upper limit
   * @return pseudorandom integer number in [0, n) range.
   */
  public int nextInt(int n) {
    return random.nextInt(n);
  }

  /**
   * Returns the next pseudorandom, uniformly distributed
   * {@code double} value between {@code 0.0} and
   * {@code 1.0} from this random number generator's sequence.
   *
   * @return pseudorandom number in [0, 1)
   */
  public double nextDouble() {
    return random.nextDouble();
  }
}
