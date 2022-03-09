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
package org.apache.giraph.jython;

import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.EnumConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.graph.Language;

/**
 * Jython related {@link org.apache.hadoop.conf.Configuration} options
 */
public class JythonOptions {
  /** Options for a graph type */
  public static class JythonGraphTypeOptions {
    /** user graph type */
    private final GraphType graphType;
    /** Option for Jython class name implementing type */
    private final StrConfOption jythonClassNameOption;
    /** Option for whether Jython type needs a wrapper */
    private final BooleanConfOption needsWrapperOption;
    /** Option for language */
    private final EnumConfOption<Language> languageOption;

    /**
     * Constructor
     *
     * @param graphType GraphType
     */
    public JythonGraphTypeOptions(GraphType graphType) {
      this.graphType = graphType;
      jythonClassNameOption = new StrConfOption("giraph.jython." +
          graphType.dotString() + ".class.name", null,
          "Name of class in Jython implementing " + graphType.spaceString());
      needsWrapperOption = new BooleanConfOption("giraph.jython." +
          graphType.dotString() + ".needs.wrapper", false,
          "Whether the " + graphType.spaceString() +
          " jython type needs a wrapper");
      languageOption = EnumConfOption.create("giraph." +
          graphType.dotString() + ".language", Language.class, Language.JAVA,
          "Language " + graphType.spaceString() + " is implemented in");
    }

    public GraphType getGraphType() {
      return graphType;
    }

    public BooleanConfOption getNeedsWrapperOption() {
      return needsWrapperOption;
    }

    public StrConfOption getJythonClassNameOption() {
      return jythonClassNameOption;
    }

    private EnumConfOption<Language> getLanguageOption() {
      return languageOption;
    }
  }

  /** vertex id options */
  public static final JythonGraphTypeOptions JYTHON_VERTEX_ID =
      new JythonGraphTypeOptions(GraphType.VERTEX_ID);

  /** vertex value options */
  public static final JythonGraphTypeOptions JYTHON_VERTEX_VALUE =
      new JythonGraphTypeOptions(GraphType.VERTEX_VALUE);

  /** edge value options */
  public static final JythonGraphTypeOptions JYTHON_EDGE_VALUE =
      new JythonGraphTypeOptions(GraphType.EDGE_VALUE);

  /** outgonig message value options */
  public static final JythonGraphTypeOptions JYTHON_OUT_MSG_VALUE =
      new JythonGraphTypeOptions(GraphType.OUTGOING_MESSAGE_VALUE);

  /** Name of Computation class in Jython script */
  public static final StrConfOption JYTHON_COMPUTATION_CLASS_NAME =
      new StrConfOption("giraph.jython.class", null,
          "Name of Computation class in Jython script");

  /** Don't construct */
  private JythonOptions() { }
}
