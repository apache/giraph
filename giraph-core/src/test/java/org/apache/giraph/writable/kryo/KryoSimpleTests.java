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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Random;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.giraph.utils.WritableUtils;
import org.junit.Test;

/**
 * Tests some subtle cases of kryo serialization.
 */
public class KryoSimpleTests {
   interface BasicProperties {
     boolean getBoolProp();
     void setBoolProp(boolean val);
     byte getByteProp();
     void setByteProp(byte val);
     short getShortProp();
     void setShortProp(short val);
     char getCharProp();
     void setCharProp(char val);
     int getIntProp();
     void setIntProp(int val);
     long getLongProp();
     void setLongProp(long val);
     float getFloatProp();
     void setFloatProp(float val);
     double getDoubleProp() ;
     void setDoubleProp(double val);
     String getStrProp() ;
     void setStrProp(String strProp);
     List getListProp();
     void setListProp(List val);
     Set getSetProp();
     void setSetProp(Set val);
     Map getMapProp() ;
     void setMapProp(Map val);
  }
  public static class TestClass implements BasicProperties{
    boolean boolProp;
    byte byteProp;
    short shortProp;
    char charProp;
    int intProp;
    long longProp;
    float floatProp;
    double doubleProp;
    String strProp;
    List listProp;
    Set setProp;
    Map mapProp;

    public TestClass() {
    }

    public boolean getBoolProp() {
      return boolProp;
    }
    public void setBoolProp(boolean val) {
      boolProp = val;
    }
    public byte getByteProp() {
      return byteProp;
    }
    public void setByteProp(byte val) {
      byteProp = val;
    }
    public short getShortProp() {
      return shortProp;
    }
    public void setShortProp(short val) {
      shortProp = val;
    }
    public char getCharProp() {
      return charProp;
    }
    public void setCharProp(char val) {
      charProp = val;
    }
    public int getIntProp() {
      return intProp;
    }
    public void setIntProp(int val) {
      intProp = val;
    }
    public long getLongProp() {
      return longProp;
    }
    public void setLongProp(long val) {
      longProp = val;
    }
    public float getFloatProp() {
      return floatProp;
    }
    public void setFloatProp(float val) {
      floatProp = val;
    }
    public double getDoubleProp() {
      return doubleProp;
    }
    public void setDoubleProp(double val) {
      doubleProp = val;
    }
    public String getStrProp() {
      return strProp;
    }
    public void setStrProp(String strProp) {
      this.strProp = strProp;
    }
    public List getListProp() {
      return this.listProp;
    }
    public void setListProp(List val) {
      this.listProp = val;
    }
    public Set getSetProp() {
      return setProp;
    }
    public void setSetProp(Set val) {
      this.setProp = val;
    }
    public Map getMapProp() {
      return mapProp;
    }
    public void setMapProp(Map val) {
      this.mapProp = val;
    }

  }

  public static class TestClassFromWritable extends KryoSimpleWritable implements BasicProperties {
    boolean boolProp;
    byte byteProp;
    short shortProp;
    char charProp;
    int intProp;
    long longProp;
    float floatProp;
    double doubleProp;
    String strProp;
    List listProp;
    Set setProp;
    Map mapProp;

    public TestClassFromWritable() {
    }
    public boolean getBoolProp() {
      return boolProp;
    }
    public void setBoolProp(boolean val) {
      boolProp = val;
    }
    public byte getByteProp() {
      return byteProp;
    }
    public void setByteProp(byte val) {
      byteProp = val;
    }
    public short getShortProp() {
      return shortProp;
    }
    public void setShortProp(short val) {
      shortProp = val;
    }
    public char getCharProp() {
      return charProp;
    }
    public void setCharProp(char val) {
      charProp = val;
    }
    public int getIntProp() {
      return intProp;
    }
    public void setIntProp(int val) {
      intProp = val;
    }
    public long getLongProp() {
      return longProp;
    }
    public void setLongProp(long val) {
      longProp = val;
    }
    public float getFloatProp() {
      return floatProp;
    }
    public void setFloatProp(float val) {
      floatProp = val;
    }
    public double getDoubleProp() {
      return doubleProp;
    }
    public void setDoubleProp(double val) {
      doubleProp = val;
    }
    public String getStrProp() {
      return strProp;
    }
    public void setStrProp(String strProp) {
      this.strProp = strProp;
    }
    public List getListProp() {
      return this.listProp;
    }
    public void setListProp(List val) {
      this.listProp = val;
    }
    public Set getSetProp() {
      return setProp;
    }
    public void setSetProp(Set val) {
      this.setProp = val;
    }
    public Map getMapProp() {
      return mapProp;
    }
    public void setMapProp(Map val) {
      this.mapProp = val;
    }

  }

  public static class TestClassFromKryoWritable extends KryoWritable implements BasicProperties {
    boolean boolProp;
    byte byteProp;
    short shortProp;
    char charProp;
    int intProp;
    long longProp;
    float floatProp;
    double doubleProp;
    String strProp;
    List listProp;
    Set setProp;
    Map mapProp;

    public TestClassFromKryoWritable() {
    }
    public boolean getBoolProp() {
      return boolProp;
    }
    public void setBoolProp(boolean val) {
      boolProp = val;
    }
    public byte getByteProp() {
      return byteProp;
    }
    public void setByteProp(byte val) {
      byteProp = val;
    }
    public short getShortProp() {
      return shortProp;
    }
    public void setShortProp(short val) {
      shortProp = val;
    }
    public char getCharProp() {
      return charProp;
    }
    public void setCharProp(char val) {
      charProp = val;
    }
    public int getIntProp() {
      return intProp;
    }
    public void setIntProp(int val) {
      intProp = val;
    }
    public long getLongProp() {
      return longProp;
    }
    public void setLongProp(long val) {
      longProp = val;
    }
    public float getFloatProp() {
      return floatProp;
    }
    public void setFloatProp(float val) {
      floatProp = val;
    }
    public double getDoubleProp() {
      return doubleProp;
    }
    public void setDoubleProp(double val) {
      doubleProp = val;
    }
    public String getStrProp() {
      return strProp;
    }
    public void setStrProp(String strProp) {
      this.strProp = strProp;
    }
    public List getListProp() {
      return this.listProp;
    }
    public void setListProp(List val) {
      this.listProp = val;
    }
    public Set getSetProp() {
      return setProp;
    }
    public void setSetProp(Set val) {
      this.setProp = val;
    }
    public Map getMapProp() {
      return mapProp;
    }
    public void setMapProp(Map val) {
      this.mapProp = val;
    }
  }

  static int collectionSize = 100000;
  private static BasicProperties populateTestClass(BasicProperties testClass) {
    Random rand = new Random();
    BasicProperties reference = new TestClass();
     reference.setBoolProp(rand.nextBoolean());
     byte [] byteArr = new byte[1];
    rand.nextBytes(byteArr);
    reference.setByteProp(byteArr[0]);
    reference.setCharProp((char)rand.nextInt((int)Character.MAX_VALUE));
    reference.setIntProp(rand.nextInt(Integer.MAX_VALUE));
    reference.setShortProp((short)rand.nextInt((int)Short.MAX_VALUE));
    reference.setLongProp(rand.nextLong());
    reference.setFloatProp(rand.nextFloat());
    reference.setDoubleProp(rand.nextDouble());
    reference.setStrProp(RandomStringUtils.randomAlphabetic(10));

    List<Integer> list = new ArrayList<>();
    for (int i=0; i < collectionSize ; i++) {
      list.add(rand.nextInt(Integer.MAX_VALUE));
    }
    reference.setListProp(list);

    Set<Integer> set = new HashSet<>();
    for (int i=0; i < collectionSize; i++) {
      set.add(rand.nextInt(Integer.MAX_VALUE));
    }
    reference.setSetProp(set);

    Map<Integer, Integer> map = new HashMap<>();
    for (int i=0; i < collectionSize; i++) {
      map.put(i, rand.nextInt(Integer.MAX_VALUE));
    }
    reference.setMapProp(map);
    popuateTestFrom(reference, testClass);
    return reference;
  }

  private static void popuateTestFrom(BasicProperties from, BasicProperties to) {
     to.setStrProp(from.getStrProp());
     to.setDoubleProp(from.getDoubleProp());
     to.setFloatProp(from.getFloatProp());
     to.setLongProp(from.getLongProp());
     to.setIntProp(from.getIntProp());
     to.setCharProp(from.getCharProp());
     to.setByteProp(from.getByteProp());
     to.setBoolProp(from.getBoolProp());
     to.setShortProp(from.getShortProp());
     to.setMapProp(from.getMapProp());
     to.setSetProp(from.getSetProp());
     to.setListProp(from.getListProp());
  }

  private static void verifyTestClass(BasicProperties testClass, BasicProperties referenceClass) {
     assertEquals(testClass.getBoolProp(), referenceClass.getBoolProp());
     assertEquals(testClass.getByteProp(), referenceClass.getByteProp());
     assertEquals(testClass.getCharProp(), referenceClass.getCharProp());
     assertEquals(testClass.getDoubleProp(), referenceClass.getDoubleProp(), 0.0001);
     assertEquals(testClass.getFloatProp(), referenceClass.getFloatProp(), 0.0001);
     assertEquals(testClass.getIntProp(), referenceClass.getIntProp());
     assertEquals(testClass.getLongProp(), referenceClass.getLongProp());
     assertEquals(testClass.getShortProp(), referenceClass.getShortProp());
     assertEquals(testClass.getStrProp(), referenceClass.getStrProp());
     assertTrue(testClass.getListProp().equals(referenceClass.getListProp()));
     assertTrue(testClass.getSetProp().equals(referenceClass.getSetProp()));
     assertTrue(testClass.getMapProp().equals(referenceClass.getMapProp()));
  }

  @Test
  public void testClassFromWritable() {
    TestClassFromWritable writableClass = new TestClassFromWritable();
    TestClassFromWritable res = new TestClassFromWritable();
    BasicProperties reference = populateTestClass(writableClass);
    WritableUtils.copyInto(
            writableClass, res, true);
    verifyTestClass(res,reference);
  }

  @Test
  public void testKryoSimpleWrapper() {
    TestClass testClass = new TestClass();
    BasicProperties reference = populateTestClass(testClass);
    TestClass res = WritableUtils.createCopy(new KryoSimpleWrapper<>(testClass)).get();
    verifyTestClass(res, reference);
  }
}
