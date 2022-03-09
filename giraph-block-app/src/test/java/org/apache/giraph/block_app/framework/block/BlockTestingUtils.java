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
package org.apache.giraph.block_app.framework.block;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import it.unimi.dsi.fastutil.ints.IntArrayList;

@SuppressWarnings({"unchecked", "rawtypes"})
public class BlockTestingUtils {

  BlockTestingUtils() { }

  private static final int NUM_TRIALS = 10;
  private static final int REPEAT_TIMES = 10;

  public static int testSequential(
      Iterable<? extends AbstractPiece> referenceImpl,
      Iterable<? extends AbstractPiece> testImpl) {
    int length = 0;

    CheckIterator checkIterator = new CheckIterator(
        referenceImpl.iterator(), testImpl.iterator());
    while (checkIterator.hasNext()) {
      checkIterator.next();
      length++;
    }

    System.out.println("Length is : " + length);
    return length;
  }

  private static boolean anyHasNext(ArrayList<? extends Iterator> arr) {
    for (Iterator t : arr) {
      if (t.hasNext()) {
        return true;
      }
    }
    return false;
  }

  private static void testRandom(int length,
                                 Iterable<? extends AbstractPiece> referenceImpl,
                                 Iterable<? extends AbstractPiece> testImpl) {
    Random rand = new Random();

    ArrayList<CheckIterator<AbstractPiece>> arr = new ArrayList<>();
    IntArrayList lengths = new IntArrayList(NUM_TRIALS);
    for (int i = 0; i < NUM_TRIALS; i++) {
      lengths.add(0);
    }
    for (int i = 0; i < NUM_TRIALS; i++) {
      arr.add(new CheckIterator(referenceImpl.iterator(), testImpl.iterator()));
    }

    int totalCount = 0;
    while (anyHasNext(arr)) {
      int index = rand.nextInt(NUM_TRIALS);
      while (!arr.get(index).hasNext()) {
        index = rand.nextInt(NUM_TRIALS);
      }
      CheckIterator it = arr.get(index);
      it.next();
      int itLength = lengths.getInt(index);
      lengths.set(index, itLength + 1);
      totalCount++;
    }
    assertEquals("TotalCount should be length * NUM_TRIALS", length * NUM_TRIALS, totalCount);
    System.out.println("Final count is : " + totalCount);
  }

  /**
   * Tests both the length of the iterator returned by the block, as-well as the deterministic behavior
   * expected by calling .iterator() against the referenceImpl.
   * @param referenceImpl : A list of pieces in the expected order
   * @param testImpl : A list of pieces to test against (the Block)
   */
  public static void testIndependence(Iterable<? extends AbstractPiece> referenceImpl,
                                      Iterable<? extends AbstractPiece> testImpl) {
    int length = testSequential(referenceImpl, testImpl);
    testRandom(length, referenceImpl, testImpl);
  }

  /**
   * Test how the block interacts with a repeatBlock. The expected result is to
   * see the pieces in referenceImpl show up REPEAT_TIMES many times.
   * @param referenceImpl : A list of pieces in the expected order
   * @param block : The block to test
   */
  public static void testNestedRepeatBlock(Iterable<? extends AbstractPiece> referenceImpl, Block block) {
    Block repeatBlock = new RepeatBlock(
      REPEAT_TIMES,
      block
    );
    testIndependence(
            Iterables.concat(Collections.nCopies(REPEAT_TIMES, referenceImpl)),
            repeatBlock
    );
  }

  public static class CheckIterator<T> implements Iterator {

    private final Iterator<T> fst;
    private final Iterator<T> snd;

    public CheckIterator(Iterator<T> fst, Iterator<T> snd) {
      this.fst = fst;
      this.snd = snd;
    }

    @Override
    public boolean hasNext() {
      boolean fstHasNxt = fst.hasNext();
      boolean sndHasNxt = snd.hasNext();
      Preconditions.checkArgument(fstHasNxt == sndHasNxt, "Expect hasNext() on " +
              "both iterators to be identical. Got: " + fst.hasNext() + " and " + snd.hasNext());
      return fstHasNxt;
    }

    @Override
    public Object next() {
      T fstNxt = fst.next();
      T sndNxt = snd.next();
      Preconditions.checkArgument(fstNxt == sndNxt, "Expect objs returned by " +
              "both iterators to be identical. Got: " + fstNxt + " and " + sndNxt);
      return fstNxt;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not implemented");
    }

  }

}
