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

package org.apache.giraph.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

/**
 * Command line options for benchmarks
 */
public class BenchmarkOption {
  /** Option for help */
  public static final BenchmarkOption HELP =
      new BenchmarkOption("h", "help", false, "Help");
  /** Option for verbose */
  public static final BenchmarkOption VERBOSE =
      new BenchmarkOption("v", "verbose", false, "Verbose");
  /** Option for number of workers */
  public static final BenchmarkOption WORKERS =
      new BenchmarkOption("w", "workers", true, "Number of workers",
          "Need to choose the number of workers (-w)");
  /** Option for number of supersteps */
  public static final BenchmarkOption SUPERSTEPS =
      new BenchmarkOption("s", "supersteps", true,
          "Supersteps to execute before finishing",
          "Need to set the number of supersteps (-s)");
  /** Option for number of vertices */
  public static final BenchmarkOption VERTICES =
      new BenchmarkOption("V", "aggregateVertices", true,
          "Aggregate vertices", "Need to set the aggregate vertices (-V)");
  /** Option for number of edges per vertex */
  public static final BenchmarkOption EDGES_PER_VERTEX =
      new BenchmarkOption("e", "edgesPerVertex", true,
          "Edges per vertex",
          "Need to set the number of edges per vertex (-e)");
  /** Option for minimum ratio of partition-local edges */
  public static final BenchmarkOption LOCAL_EDGES_MIN_RATIO =
      new BenchmarkOption(
          "l", "localEdgesMinRatio", true,
          "Minimum ratio of partition-local edges (default is 0)");
  /** Option for using Jython */
  public static final BenchmarkOption JYTHON =
      new BenchmarkOption("j", "jython", false, "Use jython implementation");
  /** Option for path to script for computation */
  public static final BenchmarkOption SCRIPT_PATH =
      new BenchmarkOption("sp", "scriptPath", true,
          "Path to script for computation, can be local or HDFS path");

  /** Short option */
  private String shortOption;
  /** Long option */
  private String longOption;
  /** True iff option requires an argument */
  private boolean hasArgument;
  /** Description of the option */
  private String description;
  /**
   * Message to print if the option is missing, null if the option is not
   * required
   */
  private String missingMessage;

  /**
   * Constructor for option which is not required
   *
   * @param shortOption Short option
   * @param longOption Long option
   * @param hasArgument True iff option requires argument
   * @param description Description of the option
   */
  public BenchmarkOption(String shortOption, String longOption,
      boolean hasArgument, String description) {
    this(shortOption, longOption, hasArgument, description, null);
  }

  /**
   * Constructor for option which is not required
   *
   * @param shortOption Short option
   * @param longOption Long option
   * @param hasArgument True iff option requires argument
   * @param description Description of the option
   * @param missingMessage Message to print if the option is missing
   */
  public BenchmarkOption(String shortOption, String longOption,
      boolean hasArgument, String description, String missingMessage) {
    this.shortOption = shortOption;
    this.longOption = longOption;
    this.hasArgument = hasArgument;
    this.description = description;
    this.missingMessage = missingMessage;
  }

  /**
   * Check if the option is required
   *
   * @return True iff the option is required
   */
  public boolean isRequired() {
    return missingMessage != null;
  }

  /**
   * Add option to cli Options
   *
   * @param options Cli Options
   */
  public void addToOptions(Options options) {
    options.addOption(shortOption, longOption, hasArgument, description);
  }

  /**
   * If option is not required just return true.
   * If option is required, check if it's present in CommandLine,
   * and if it's not print the missingMessage to log.
   *
   * @param cmd CommandLine
   * @param log Logger to print the missing message to
   * @return False iff the option is required but is not specified in cmd
   */
  public boolean checkOption(CommandLine cmd, Logger log) {
    if (!isRequired()) {
      return true;
    }
    if (!cmd.hasOption(shortOption)) {
      log.info(missingMessage);
      return false;
    }
    return true;
  }

  /**
   * Check if the option is present in CommandLine
   *
   * @param cmd CommandLine
   * @return True iff the option is present in CommandLine
   */
  public boolean optionTurnedOn(CommandLine cmd) {
    return cmd.hasOption(shortOption);
  }

  /**
   * Retrieve the argument, if any, of this option
   *
   * @param cmd CommandLine
   * @return Value of the argument if option is set and has an argument,
   * otherwise null
   */
  public String getOptionValue(CommandLine cmd) {
    return cmd.getOptionValue(shortOption);
  }

  /**
   * Retrieve the argument of this option as integer value
   *
   * @param cmd CommandLine
   * @return Value of the argument as integer value
   */
  public int getOptionIntValue(CommandLine cmd) {
    return Integer.parseInt(getOptionValue(cmd));
  }

  /**
   * Retrieve the argument of this option as integer value,
   * or default value if option is not set
   *
   * @param cmd CommandLine
   * @param defaultValue Default value
   * @return Value of the argument as integer value,
   * or default value if option is not set
   */
  public int getOptionIntValue(CommandLine cmd, int defaultValue) {
    return optionTurnedOn(cmd) ? getOptionIntValue(cmd) : defaultValue;
  }

  /**
   * Retrieve the argument of this option as long value
   *
   * @param cmd CommandLine
   * @return Value of the argument as long value
   */
  public long getOptionLongValue(CommandLine cmd) {
    return Long.parseLong(getOptionValue(cmd));
  }

  /**
   * Retrieve the argument of this option as float value
   *
   * @param cmd CommandLine
   * @return Value of the argument as float value
   */
  public float getOptionFloatValue(CommandLine cmd) {
    return Float.parseFloat(getOptionValue(cmd));
  }

  /**
   * Retrieve the argument of this option as float value,
   * or default value if option is not set
   *
   * @param cmd CommandLine
   * @param defaultValue Default value
   * @return Value of the argument as float value,
   * or default value if option is not set
   */
  public float getOptionFloatValue(CommandLine cmd, float defaultValue) {
    return optionTurnedOn(cmd) ? getOptionFloatValue(cmd) : defaultValue;
  }
}
