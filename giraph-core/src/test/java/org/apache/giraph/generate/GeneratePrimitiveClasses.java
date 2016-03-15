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
package org.apache.giraph.generate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import freemarker.core.ParseException;
import freemarker.template.Configuration;
import freemarker.template.MalformedTemplateNameException;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateNotFoundException;

/**
 * <p>Code generation utility that generates set of classes from a template files.
 * Templates are found in giraph-core/template/ folder.
 * If you want to add new generation, look at the main function, and add a call
 * to appropriate generate* function.</p>
 *
 * <p>Templates are using freemarker template format, which given
 * template file and map of String->Object replacements generates
 * new file.</p>
 * Main rules:
 * <ul>
 * <li><code>${something}</code> gets replaced with value of <code>map.get("something")</code></li>
 * <li><code>${obj.method}</code> gets replaced with value of <code>map.get("obj").getMethod()</code></li>
 * </ul>
 * More description about template format can be found at:
 * <a href="http://freemarker.org/docs/dgui_quickstart_template.html">tutorial</a>
 */
public class GeneratePrimitiveClasses {
  // No Short since there is no ShortWritable for some reason
  public static enum PrimitiveType {
    BOOLEAN("Boolean", false, false, false),
    BYTE("Byte", true, false, false),
    INT("Int", "Integer", true, true, false),
    LONG("Long", true, true, false),
    FLOAT("Float", true, false, true),
    DOUBLE("Double", true, false, true);

    private final String name;
    private final String boxed;
    private final boolean numeric;
    private final boolean id;
    private final boolean floating;

    private PrimitiveType(String name, String boxed, boolean numeric, boolean id, boolean floating) {
      this.name = name;
      this.boxed = boxed;
      this.numeric = numeric;
      this.id = id;
      this.floating = floating;
    }

    private PrimitiveType(String name, boolean numeric, boolean id, boolean floating) {
      this(name, name, numeric, id, floating);
    }

    public String getName() {
      return name;
    }

    public String getCamel() {
      return name;
    }

    public String getLower() {
      return name.toLowerCase();
    }

    public String getBoxed() {
      return boxed;
    }

    public boolean isNumeric() {
      return numeric;
    }

    public boolean isId() {
      return id;
    }

    public boolean isFloating() {
      return floating;
    }
  }

  public static void main(String[] args) throws Exception {
    /* Create and adjust the configuration singleton */
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
    cfg.setDirectoryForTemplateLoading(new File("templates"));
    cfg.setDefaultEncoding("UTF-8");
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

    generateForAll(
        cfg,
        EnumSet.allOf(PrimitiveType.class),
        "TypeConsumer.java",
        "src/main/java/org/apache/giraph/function/primitive/%sConsumer.java");

    generateForAll(
        cfg,
        EnumSet.allOf(PrimitiveType.class),
        "Obj2TypeFunction.java",
        "src/main/java/org/apache/giraph/function/primitive/Obj2%sFunction.java");

//    generateForAll(
//        cfg,
//        EnumSet.allOf(PrimitiveType.class),
//        "TypeTypeOps.java",
//        "src/main/java/org/apache/giraph/types/ops/%sTypeOps.java");
//
//    generateForAll(
//        cfg,
//        EnumSet.allOf(PrimitiveType.class),
//        "WTypeArrayList.java",
//        "src/main/java/org/apache/giraph/types/ops/collections/array/W%sArrayList.java");

    EnumSet<PrimitiveType> ids = EnumSet.noneOf(PrimitiveType.class);
    for (PrimitiveType type : PrimitiveType.values()) {
      if (type.isId()) {
        ids.add(type);
      }
    }
  }

  /**
   * Generate a set of files from a template, one for each type in the passed set,
   * where added entry for "type" to that object is added, on top of default entries.
   */
  private static void generateForAll(Configuration cfg, EnumSet<PrimitiveType> types,
      String template, String outputPattern) throws TemplateNotFoundException, MalformedTemplateNameException, ParseException, FileNotFoundException, IOException, TemplateException {
    for (PrimitiveType type : types) {
      Map<String, Object> props = defaultMap();
      props.put("type", type);
      generateAndWrite(cfg, props,
          template,
          String.format(outputPattern, type.getCamel()));
    }
  }

  /** Generate a single file from a template, replacing mappings from given properties */
  private static void generateAndWrite(Configuration cfg, Map<String, Object> props,
      String template, String outputFile)
          throws TemplateNotFoundException, MalformedTemplateNameException, ParseException,
          IOException, FileNotFoundException, TemplateException {
    Template temp = cfg.getTemplate(template);
    Writer out = new OutputStreamWriter(new FileOutputStream(outputFile));
    temp.process(props, out);
    out.close();
  }

  public static Map<String, Object> defaultMap() {
    Map<String, Object> props = new HashMap<>();
    props.put("generated_message", GENERATED_MESSAGE);
    return props;
  }

  private static final String GENERATED_MESSAGE =
      "// AUTO-GENERATED class via class:\n// " + GeneratePrimitiveClasses.class.getName();
}
