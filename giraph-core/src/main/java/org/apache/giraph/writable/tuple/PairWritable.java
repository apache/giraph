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
package org.apache.giraph.writable.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Writable;

/**
 * Pair Writable class, that knows types upfront and can deserialize itself.
 *
 * PairWritable knows types, as instances are passed through constructor, and
 * are references are immutable (values themselves are mutable).
 *
 * Child classes specify no-arg constructor that passes concrete types in.
 *
 * Extends Pair, not ImmutablePair, since later class is final. Code is
 * copied from it.
 *
 * @param <L> Type of the left element
 * @param <R> Type of the right element
 */
public class PairWritable<L extends Writable, R extends Writable>
    extends Pair<L, R> implements Writable {
  /** Left object */
  private final L left;
  /** Right object */
  private final R right;

  /**
   * Create a new pair instance.
   *
   * @param left  the left value
   * @param right  the right value
   */
  public PairWritable(L left, R right) {
    this.left = left;
    this.right = right;
  }

  /**
   * <p>
   * Obtains an immutable pair of from two objects inferring
   * the generic types.</p>
   *
   * <p>This factory allows the pair to be created using inference to
   * obtain the generic types.</p>
   *
   * @param <L> the left element type
   * @param <R> the right element type
   * @param left  the left element, may be null
   * @param right  the right element, may be null
   * @return a pair formed from the two parameters, not null
   */
  public static <L extends Writable, R extends Writable>
  PairWritable<L, R> of(L left, R right) {
    return new PairWritable<L, R>(left, right);
  }

  @Override
  public final L getLeft() {
    return left;
  }

  @Override
  public final R getRight() {
    return right;
  }

  /**
   * <p>Throws {@code UnsupportedOperationException}.</p>
   *
   * <p>This pair is immutable, so this operation is not supported.</p>
   *
   * @param value  the value to set
   * @return never
   * @throws UnsupportedOperationException as this operation is not supported
   */
  @Override
  public final R setValue(R value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    left.write(out);
    right.write(out);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    left.readFields(in);
    right.readFields(in);
  }
}
