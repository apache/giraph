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

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.library.striping.StripingUtils;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.function.primitive.Obj2IntFunction;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;


public class KryoWritableWrapperJava8Test {
  // Copy from KryoWritableWrapperTest, since we cannot extend it, since tests are not in jars
  public static <T> T kryoSerDeser(T t) throws IOException {
    KryoWritableWrapper<T> wrapped = new KryoWritableWrapper<>(t);
    KryoWritableWrapper<T> deser = new KryoWritableWrapper<>();
    WritableUtils.copyInto(wrapped, deser, true);
    return deser.get();
  }

  @Test(expected = RuntimeException.class)
  public void testNonSerializableLambda() throws IOException {
    Runnable nonCapturing = () -> System.out.println("works");
    kryoSerDeser(nonCapturing).run();
  }

  @Test
  public void testLambda() throws IOException {
    Runnable nonCapturing = (Runnable & Serializable) () -> System.out.println("works");
    kryoSerDeser(nonCapturing).run();

    String works = "works";
    Runnable capturing = (Runnable & Serializable) () -> System.out.println(works);
    kryoSerDeser(capturing).run();
  }

  @Test
  public void testLambdaCapturingThisRef() throws IOException {
    KryoWritableWrapperJava8Test o = this;
    Runnable capturingThisRef = (Runnable & Serializable) () ->
      System.out.println(o);
    kryoSerDeser(capturingThisRef).run();
  }

  @Test
  public void testLambdaCapturingThis() throws IOException {
    Runnable capturingThis = (Runnable & Serializable) () ->
      System.out.println(this);
    kryoSerDeser(capturingThis).run();
  }

  @Test
  public void testLambdaCapturingLambda() throws IOException {
    Supplier<Boolean> nonCapturing = () -> true;
    Runnable capturingLambda = (Runnable & Serializable) () ->
      System.out.println(nonCapturing);
    kryoSerDeser(capturingLambda).run();
  }

  @Test
  public void testLambdaCapturingLambdaWithCapture() throws IOException {
    boolean trueVar = new Random().nextDouble() < 1;
    Supplier<Boolean> capturing = () -> trueVar;
    Runnable capturingLambda = (Runnable & Serializable) () ->
      System.out.println(capturing + " " + trueVar);
    kryoSerDeser(capturingLambda).run();
  }


  @Test
  public void testLambdaFunctions() throws IOException {
    Supplier<Boolean> nonCapturing = () -> true;
    Assert.assertTrue(kryoSerDeser(nonCapturing).get());

    boolean trueVar = new Random().nextDouble() < 1;
    Supplier<Boolean> capturing = () -> trueVar;
    Assert.assertTrue(kryoSerDeser(capturing).get());
  }

  @Test
  public void testLambdasFromCode() throws IOException {
    Assert.assertNotNull(kryoSerDeser(StripingUtils.fastHashStriping(3)));
    Assert.assertNotNull(kryoSerDeser(StripingUtils.fastHashStripingPredicate(3)));

    Assert.assertNotNull(kryoSerDeser(
        (Int2ObjFunction<Obj2IntFunction<LongWritable>>) StripingUtils::fastHashStriping));

    Int2ObjFunction<Int2ObjFunction<Predicate<LongWritable>>> stripingPredicate = StripingUtils::fastHashStripingPredicate;
    Assert.assertNotNull(kryoSerDeser(stripingPredicate));

    Assert.assertNotNull(kryoSerDeser(stripingPredicate.apply(3)));

    Assert.assertNotNull(kryoSerDeser(StripingUtils.fastHashStripingPredicate(3).apply(2)));

    Assert.assertNotNull(kryoSerDeser(stripingPredicate.apply(3).apply(2)));

    Predicate<LongWritable> predicate = stripingPredicate.apply(3).apply(2);
    Assert.assertNotNull(kryoSerDeser(predicate));

    Runnable capturingLambda = (Runnable & Serializable) () ->
      System.out.println(predicate.apply(new LongWritable()));
    kryoSerDeser(capturingLambda).run();

    Assert.assertNotNull(kryoSerDeser(
      StripingUtils.generateStripedBlock(
          5,
          (filter) -> new Block() {
            private final Predicate<LongWritable> test = filter;
            {
              Assert.assertTrue(filter instanceof Serializable);
            }

            @Override
            public Iterator<AbstractPiece> iterator() {
              return null;
            }

            @Override
            public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) { }

            @Override
            public PieceCount getPieceCount() {
              return PieceCount.createUnknownCount();
            }
          })));
  }

  @Test
  public void testLambdaCapturingSameReference() throws IOException {
    ObjectTransfer<Integer> transfer = new ObjectTransfer<>();

    Consumer<Integer> consumer = (t) -> transfer.apply(t);
    Supplier<Integer> supplier = () -> transfer.get();

    class TwoLambdaObject {
      Consumer<Integer> consumer;
      Supplier<Integer> supplier;

      public TwoLambdaObject(Consumer<Integer> consumer, Supplier<Integer> supplier) {
        this.consumer = consumer;
        this.supplier = supplier;
      }
    }

    TwoLambdaObject object = new TwoLambdaObject(consumer, supplier);
    // test transfer before serialization
    object.consumer.apply(5);
    Assert.assertEquals(5, object.supplier.get().intValue());

    // test transfer through serialization
    object.consumer.apply(6);
    TwoLambdaObject deser = kryoSerDeser(object);
    Assert.assertEquals(6, deser.supplier.get().intValue());

    // test that after serialization, both lambdas point to the same object
    deser.consumer.apply(4);
    Assert.assertEquals(4, deser.supplier.get().intValue());
  }

  // Bug in Java, have test to know when it becomes fixed
  @Test //(expected=RuntimeException.class)
  public void testNestedLambda() throws IOException {
    Int2ObjFunction<Int2ObjFunction<Integer>> f = (x) -> (y) -> x+y;
    Assert.assertNotNull(kryoSerDeser(f));
    Assert.assertNotNull(kryoSerDeser(f.apply(0)));
    Assert.assertNotNull(kryoSerDeser(f.apply(0).apply(1)));
  }
}
