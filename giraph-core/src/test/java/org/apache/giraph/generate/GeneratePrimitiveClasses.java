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
 * template file and map of String-&gt;Object replacements generates
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
  public static enum PrimitiveType {
    BOOLEAN("Boolean"),
    BYTE("Byte"),
    SHORT("Short"),
    INT("Int"),
    LONG("Long"),
    FLOAT("Float"),
    DOUBLE("Double");

    private final String name;
    private final String nameLower;
    private final String boxed;
    private final boolean numeric;
    private final boolean id;
    private final boolean floating;
    private final boolean hasWritable;

    private PrimitiveType(String name) {
      this.name = name;
      this.nameLower = name.toLowerCase();
      this.boxed = "Int".equals(name) ? "Integer" : name;
      this.numeric = !"Boolean".equals(name);
      this.id = "Int".equals(name) || "Long".equals(name);
      this.floating = "Float".equals(name) || "Double".equals(name);
      // For some reason there is no ShortWritable in current Hadoop version
      this.hasWritable = !"Short".equals(name);
    }

    public String getName() {
      return name;
    }

    public String getCamel() {
      return name;
    }

    public String getLower() {
      return nameLower;
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

    public boolean hasWritable() {
      return hasWritable;
    }
  }

  public static void main(String[] args) throws Exception {
    /* Create and adjust the configuration singleton */
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
    cfg.setDirectoryForTemplateLoading(new File("templates"));
    cfg.setDefaultEncoding("UTF-8");
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

    String[] primitiveFunctions = {
      "%sConsumer", "%sPredicate", "Obj2%sFunction", "%s2ObjFunction", "%s2%sFunction"
    };

    for (String function: primitiveFunctions) {
      generateForAll(
          cfg,
          EnumSet.allOf(PrimitiveType.class),
          function.replaceAll("\\%s", "Type") + ".java",
          "src/main/java/org/apache/giraph/function/primitive/" + function + ".java");
    }


    generateForAll(
        cfg,
        EnumSet.allOf(PrimitiveType.class),
        "TypeComparatorFunction.java",
        "../giraph-block-app-8/src/main/java/org/apache/giraph/function/primitive/comparators/%sComparatorFunction.java");

    EnumSet<PrimitiveType> writableSet = EnumSet.noneOf(PrimitiveType.class);
    EnumSet<PrimitiveType> ids = EnumSet.noneOf(PrimitiveType.class);
    EnumSet<PrimitiveType> numerics = EnumSet.noneOf(PrimitiveType.class);
    for (PrimitiveType type : EnumSet.allOf(PrimitiveType.class)) {
      if (type.hasWritable()) {
        writableSet.add(type);
        if (type.isId()) {
          ids.add(type);
        }
        if (type.isNumeric()) {
          numerics.add(type);
        }
      }
    }

    generateForAll(
        cfg,
        writableSet,
        "TypeTypeOps.java",
        "src/main/java/org/apache/giraph/types/ops/%sTypeOps.java");

    generateForAll(
        cfg,
        writableSet,
        "WTypeCollection.java",
        "src/main/java/org/apache/giraph/types/ops/collections/W%sCollection.java");

    generateForAll(
        cfg,
        writableSet,
        "WTypeArrayList.java",
        "src/main/java/org/apache/giraph/types/ops/collections/array/W%sArrayList.java");

    generateForAll(
        cfg,
        writableSet,
        writableSet,
        "TypeTypeConsumer.java",
        "src/main/java/org/apache/giraph/function/primitive/pairs/%s%sConsumer.java");

    generateForAll(
        cfg,
        writableSet,
        writableSet,
        "TypeTypePredicate.java",
        "src/main/java/org/apache/giraph/function/primitive/pairs/%s%sPredicate.java");

    generateForAll(
        cfg,
        ids,
        numerics,
        "Type2TypeMapEntryIterable.java",
        "src/main/java/org/apache/giraph/types/heaps/%s2%sMapEntryIterable.java");

    generateForAll(
        cfg,
        ids,
        numerics,
        "FixedCapacityType2TypeMinHeap.java",
        "src/main/java/org/apache/giraph/types/heaps/FixedCapacity%s%sMinHeap.java");

    generateForAll(
        cfg,
        ids,
        numerics,
        "TestFixedCapacityType2TypeMinHeap.java",
        "src/test/java/org/apache/giraph/types/heaps/TestFixedCapacity%s%sMinHeap.java");

    System.out.println("Successfully generated classes");
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
          outputPattern.replaceAll("\\%s", type.getCamel()));
    }
  }

  /**
   * Generate a set of files from a template, one for each combination of
   * types in the passed sets, where added entry for "type1" and "type2" to
   * that object is added, on top of default entries.
   */
  private static void generateForAll(Configuration cfg,
      EnumSet<PrimitiveType> types1, EnumSet<PrimitiveType> types2,
      String template, String outputPattern) throws TemplateNotFoundException, MalformedTemplateNameException, ParseException, FileNotFoundException, IOException, TemplateException {
    for (PrimitiveType type1 : types1) {
      for (PrimitiveType type2 : types2) {
        Map<String, Object> props = defaultMap();
        props.put("type1", type1);
        props.put("type2", type2);
        generateAndWrite(cfg, props,
            template,
            String.format(outputPattern, type1.getCamel(), type2.getCamel()));
      }
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
