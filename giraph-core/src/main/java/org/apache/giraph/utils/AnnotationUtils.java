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

package org.apache.giraph.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Helper class to deal with annotations in runtime.
 */
public class AnnotationUtils {
  /** Do not instantiate. */
  private AnnotationUtils() {
  }

  /**
   * Finds all classes within a package which are annotated with certain
   * annotation.
   *
   * @param annotation  Annotation which we are looking for
   * @param <T>         Annotation class
   * @param packageName Package in which to search
   * @return The list of annotated classes
   */
  public static <T extends Annotation> List<Class<?>> getAnnotatedClasses(
      Class<T> annotation, String packageName) {
    ArrayList<Class<?>> ret = new ArrayList<Class<?>>();
    for (Iterator<Class<?>> it = getClassesIterator(packageName);
         it.hasNext();) {
      Class<?> clazz = it.next();
      if (clazz.getAnnotation(annotation) != null) {
        ret.add(clazz);
      }
    }
    return ret;
  }

  /**
   * @param packageName Package through which to iterate
   * @return Iterator through the classes of this jar file (if executed from
   *         jar) or through the classes of org package (if .class file is
   *         executed)
   */
  public static Iterator<Class<?>> getClassesIterator(String packageName) {
    if (isExecutedFromJar()) {
      try {
        return new JarClassesIterator(packageName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new GeneralClassesIterator(packageName);
    }
  }

  /**
   * @return Whether or not the code is executed from jar file
   */
  private static boolean isExecutedFromJar() {
    return AnnotationUtils.class.getResource("AnnotationUtils.class")
        .getProtocol().equals("jar");
  }

  /**
   * To be used when {@link #isExecutedFromJar()}  is true.
   *
   * @return If executed from jar file returns the path to the jar
   *         (otherwise it will return the folder in which this .class file is
   *         located)
   */
  private static String getCurrentJar() {
    return AnnotationUtils.class.getProtectionDomain().getCodeSource()
        .getLocation().getFile();
  }

  /**
   * To be used when {@link #isExecutedFromJar()}  is true.
   * <p/>
   * Iterator through classes of this jar file.
   */
  private static class JarClassesIterator implements Iterator<Class<?>> {
    /** Used to go through classes in current jar */
    private final JarInputStream jarIn;
    /** Are we positioned on the next class entry */
    private boolean entryLoaded;
    /** Next class entry */
    private JarEntry currentEntry;
    /** Folder in which to look */
    private final String path;

    /**
     * @param packageName Package through which to iterate
     * @throws IOException
     */
    public JarClassesIterator(String packageName) throws IOException {
      jarIn = new JarInputStream(new FileInputStream(new File(
          getCurrentJar())));
      entryLoaded = false;
      currentEntry = null;
      path = packageName.replace(".", File.separator);
    }

    @Override
    public boolean hasNext() {
      loadNextEntry();
      return currentEntry != null;
    }

    @Override
    public Class<?> next() {
      loadNextEntry();
      if (currentEntry == null) {
        throw new NoSuchElementException();
      }
      entryLoaded = false;

      String className = currentEntry.getName().replace(".class",
          "").replace(File.separator, ".");
      return loadClass(className);
    }

    /**
     * Sets position to next class entry
     */
    private void loadNextEntry() {
      while (!entryLoaded) {
        try {
          currentEntry = jarIn.getNextJarEntry();
          if (currentEntry == null || (currentEntry.getName().endsWith(
              ".class") && (currentEntry.getName().startsWith(path)))) {
            entryLoaded = true;
          } else {
            currentEntry = null;
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      if (currentEntry == null) {
        try {
          jarIn.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Can't remove from classes iterator");
    }
  }

  /**
   * To be used when {@link #isExecutedFromJar()}  is false.
   * <p/>
   * Iterator through classes of some package.
   */
  private static class GeneralClassesIterator implements Iterator<Class<?>> {
    /** From which position in path the package name starts */
    private final int stringPosition;
    /** Recursive directory iterator */
    private final Iterator<File> iterator;

    /**
     * @param packageName Package through which to iterate
     */
    public GeneralClassesIterator(String packageName) {
      String mainPath = AnnotationUtils.class.getProtectionDomain()
          .getCodeSource().getLocation().getFile();
      String subPath = packageName.replace(".", File.separator);
      File directory = new File(mainPath + subPath);
      stringPosition = directory.getPath().length() - packageName.length();
      List<File> files = new ArrayList<File>();
      addAllClassFiles(directory, files);
      iterator = files.iterator();
    }

    /**
     * Recursively add all .class files from the directory to the list.
     *
     * @param directory Directory from which we are adding files
     * @param files List we add files to
     */
    private void addAllClassFiles(File directory, List<File> files) {
      for (File file : directory.listFiles()) {
        if (file.isDirectory()) {
          addAllClassFiles(file, files);
        } else if (file.getName().endsWith(".class")) {
          files.add(file);
        }
      }
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Class<?> next() {
      String className = iterator.next().getPath().substring(stringPosition)
          .replace(".class", "").replace(File.separator, ".");
      return loadClass(className);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Can't remove from classes iterator");
    }
  }

  /**
   * Loads the class with the specified name
   *
   * @param className Name of the class we are loading
   * @return Class with the specified name
   */
  private static Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Error loading class " + className, e);
    } catch (NoClassDefFoundError e) {
      throw new RuntimeException("Error loading class " + className, e);
    }
  }
}
