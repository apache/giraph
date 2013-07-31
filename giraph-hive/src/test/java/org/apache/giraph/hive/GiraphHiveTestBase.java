package org.apache.giraph.hive;

import org.junit.BeforeClass;

import com.facebook.hiveio.log.LogHelpers;

public class GiraphHiveTestBase {
  @BeforeClass
  public static void silenceLoggers() {
    LogHelpers.silenceLoggers();
  }
}
