package org.apache.giraph.hive;

import org.junit.BeforeClass;

public class GiraphHiveTestBase {
  @BeforeClass
  public static void silenceLoggers() {
    com.facebook.hiveio.testing.Helpers.silenceLoggers();
  }
}
