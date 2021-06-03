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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Writer;

/**
 * Helper class for filesystem operations during testing
 */
public class FileUtils {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(FileUtils.class);

  /**
   * Utility class should not be instantiatable
   */
  private FileUtils() {
  }

  /**
   * Create a temporary folder that will be removed after the test.
   *
   * @param computationName Used for generating the folder name.
   * @return File object for the directory.
   */
  public static File createTestDir(String computationName)
    throws IOException {
    String systemTmpDir = System.getProperty("java.io.tmpdir");
    long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
    File testTempDir = new File(systemTmpDir, "giraph-" +
        computationName + '-' + simpleRandomLong);
    if (!testTempDir.mkdir()) {
      throw new IOException("Could not create " + testTempDir);
    }
    testTempDir.deleteOnExit();
    return testTempDir;
  }

  /**
   * Make a temporary file.
   *
   * @param parent Parent directory.
   * @param name File name.
   * @return File object to temporary file.
   * @throws IOException
   */
  public static File createTempFile(File parent, String name)
    throws IOException {
    return createTestTempFileOrDir(parent, name, false);
  }

  /**
   * Make a temporary directory.
   *
   * @param parent Parent directory.
   * @param name Directory name.
   * @return File object to temporary file.
   * @throws IOException
   */
  public static File createTempDir(File parent, String name)
    throws IOException {
    File dir = createTestTempFileOrDir(parent, name, true);
    if (!dir.delete()) {
      LOG.error("createTempDir: Failed to create directory " + dir);
    }
    return dir;
  }

  /**
   * Create a test temp file or directory.
   *
   * @param parent Parent directory
   * @param name Name of file
   * @param dir Is directory?
   * @return File object
   * @throws IOException
   */
  public static File createTestTempFileOrDir(File parent, String name,
    boolean dir) throws IOException {
    File f = new File(parent, name);
    f.deleteOnExit();
    if (dir && !f.mkdirs()) {
      throw new IOException("Could not make directory " + f);
    }
    return f;
  }

  /**
   * Write lines to a file.
   *
   * @param file File to write lines to
   * @param lines Strings written to the file
   * @throws IOException
   */
  public static void writeLines(File file, String[] lines)
    throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    } finally {
      Closeables.close(writer, true);
    }
  }

  /**
   * Recursively delete a directory
   *
   * @param dir Directory to delete
   */
  public static void delete(File dir) {
    if (dir != null) {
      new DeletingVisitor().accept(dir);
    }
  }

  /**
   * Deletes files.
   */
  private static class DeletingVisitor implements FileFilter {
    @Override
    public boolean accept(File f) {
      if (!f.isFile()) {
        if (f.listFiles(this) == null) {
          LOG.error("accept: Failed to list files of " + f);
        }
      }
      if (!f.delete()) {
        LOG.error("accept: Failed to delete file " + f);
      }
      return false;
    }
  }

  /**
   * Helper method to remove a path if it exists.
   *
   * @param conf Configuration to load FileSystem from
   * @param path Path to remove
   * @throws IOException
   */
  public static void deletePath(Configuration conf, String path)
    throws IOException {
    deletePath(conf, new Path(path));
  }

  /**
   * Helper method to remove a path if it exists.
   *
   * @param conf Configuration to load FileSystem from
   * @param path Path to remove
   * @throws IOException
   */
  public static void deletePath(Configuration conf, Path path)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.delete(path, true);
  }
}
