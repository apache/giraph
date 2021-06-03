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
package org.apache.giraph.block_app.library.striping;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.FilteringBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.function.primitive.Obj2IntFunction;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Utility functions for doing superstep striping.
 *
 * We need to make sure that partitioning (which uses mod for distributing
 * data across workers) is independent from striping itself. So we are using
 * fastHash function below, taken from https://code.google.com/p/fast-hash/.
 */
public class StripingUtils {
  private StripingUtils() { }

  /* The MIT License

  Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation
  files (the "Software"), to deal in the Software without
  restriction, including without limitation the rights to use, copy,
  modify, merge, publish, distribute, sublicense, and/or sell copies
  of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
  */
  /**
   * Returns 32-bit hash of a given value.
   *
   * Fast and generally good hashing function, adapted from C++ implementation:
   * https://code.google.com/p/fast-hash/
   */
  public static int fastHash(long h) {
    h ^= h >> 23;
    h *= 0x2127599bf4325c37L;
    h ^= h >> 47;
    return ((int) (h - (h >> 32))) & 0x7fffffff;
  }

  /**
   * Returns number in [0, stripes) range, from given input {@code value}.
   */
  public static int fastStripe(long value, int stripes) {
    return fastHash(value) % stripes;
  }

  /**
   * Fast hash-based striping for LongWritable IDs, returns a function
   * that for a given ID returns it's stripe index.
   */
  public static
  Obj2IntFunction<LongWritable> fastHashStriping(final int stripes) {
    return new Obj2IntFunction<LongWritable>() {
      @Override
      public int apply(LongWritable id) {
        return fastStripe(id.get(), stripes);
      }
    };
  }

  /**
   * Fast hash-based striping for LongWritable IDs, returns a function
   * that for a given stripe index returns a predicate checking whether ID is
   * in that stripe.
   */
  public static
  Int2ObjFunction<Predicate<LongWritable>> fastHashStripingPredicate(
      final int stripes) {
    return new Int2ObjFunction<Predicate<LongWritable>>() {
      @Override
      public Predicate<LongWritable> apply(final int stripe) {
        return new Predicate<LongWritable>() {
          @Override
          public boolean apply(LongWritable id) {
            return fastStripe(id.get(), stripes) == stripe;
          }
        };
      }
    };
  }

  /**
   * Generate striped block, with given number of {@code stripes},
   * using given {@code blockGenerator} to generate block for each stripe.
   *
   * @param stripes Number of stripes
   * @param blockGenerator Function given predicate representing whether
   *                       ID is in current stripe, should return Block
   *                       for current stripe
   * @return Resulting block
   */
  public static Block generateStripedBlock(
      int stripes,
      Function<Predicate<LongWritable>, Block> blockGenerator) {
    return generateStripedBlockImpl(
        stripes, blockGenerator,
        StripingUtils.fastHashStripingPredicate(stripes));
  }

  /**
   * Generate striped block, with given number of {@code stripes},
   * using given {@code blockGenerator} to generate block for each stripe,
   * and using striping based on given {@code stripeSupplier}.
   *
   * @param stripes Number of stripes
   * @param blockGenerator Function given predicate representing whether
   *                       ID is in current stripe, should return Block
   *                       for current stripe
   * @param stripeSupplier Function given number of stripes,
   *                       generates a function that given stripe index,
   *                       returns predicate checking whether ID is in that
   *                       stripe.
   * @return Resulting block
   */
  public static <I extends WritableComparable>
  Block generateStripedBlock(
      int stripes,
      Function<Predicate<I>, Block> blockGenerator,
      Int2ObjFunction<Int2ObjFunction<Predicate<I>>> stripeSupplier) {
    return generateStripedBlockImpl(
        stripes, blockGenerator, stripeSupplier.apply(stripes));
  }

  /**
   * Stripe given block, by calling vertexSend only in it's corresponding
   * stripe. All other methods are called number of stripes times.
   *
   * @param stripes Number of stripes
   * @param block Block to stripe
   * @return Resulting block
   */
  public static Block stripeBlockBySenders(
      int stripes,
      Block block) {
    return generateStripedBlockImpl(
        stripes,
        StripingUtils.<LongWritable>createSingleStripeBySendersFunction(block),
        StripingUtils.fastHashStripingPredicate(stripes));
  }

  /**
   * Given a block, creates a function that will given a predicate filter
   * calls to vertexSend function based on that predicate.
   *
   * Useful to be combined with generateStripedBlock to stripe blocks.
   */
  public static <I extends WritableComparable> Function<Predicate<I>, Block>
      createSingleStripeBySendersFunction(final Block block) {
    return new Function<Predicate<I>, Block>() {
      @Override
      public Block apply(final Predicate<I> stripePredicate) {
        return FilteringBlock.createSendFiltering(
            new SupplierFromVertex<I, Writable, Writable, Boolean>() {
              @Override
              public Boolean get(Vertex<I, Writable, Writable> vertex) {
                return stripePredicate.apply(vertex.getId());
              }
            }, block);
      }
    };
  }

  private static <I extends WritableComparable>
  Block generateStripedBlockImpl(
      int stripes,
      Function<Predicate<I>, Block> blockGenerator,
      Int2ObjFunction<Predicate<I>> stripeSupplier) {
    Preconditions.checkArgument(stripes >= 1);
    if (stripes == 1) {
      return blockGenerator.apply(new Predicate<I>() {
        @Override
        public boolean apply(I input) {
          return true;
        }
      });
    }
    Block[] blocks = new Block[stripes];
    for (int i = 0; i < stripes; i++) {
      blocks[i] = blockGenerator.apply(stripeSupplier.apply(i));
    }
    return new SequenceBlock(blocks);
  }
}
