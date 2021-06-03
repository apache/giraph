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
package org.apache.giraph.scripting;

import org.apache.giraph.conf.JsonStringConfOption;
import org.apache.giraph.graph.Language;
import org.apache.giraph.jython.JythonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.giraph.utils.DistributedCacheUtils.getLocalCacheFile;

/**
 * Loads scripts written by user in other languages, for example Jython.
 */
public class ScriptLoader {
  /** Option for scripts to load on workers */
  public static final JsonStringConfOption SCRIPTS_TO_LOAD =
      new JsonStringConfOption("giraph.scripts.to.load",
          "Scripts to load on workers");

  /** Scripts that were loaded */
  private static final List<DeployedScript> LOADED_SCRIPTS =
      Lists.newArrayList();

  /** Logger */
  private static final Logger LOG = Logger.getLogger(ScriptLoader.class);

  /** Don't construct */
  private ScriptLoader() { }

  /**
   * Deploy a script
   *
   * @param conf {@link Configuration}
   * @param scriptPath Path to script
   * @param deployType type of deployment
   * @param language programming language
   */
  public static void setScriptsToLoad(Configuration conf,
      String scriptPath, DeployType deployType, Language language) {
    DeployedScript deployedScript = new DeployedScript(scriptPath,
        deployType, language);
    setScriptsToLoad(conf, deployedScript);
  }

  /**
   * Deploy pair of scripts
   *
   * @param conf {@link Configuration}
   * @param script1 Path to script
   * @param deployType1 type of deployment
   * @param language1 programming language
   * @param script2 Path to script
   * @param deployType2 type of deployment
   * @param language2 programming language
   */
  public static void setScriptsToLoad(Configuration conf,
      String script1, DeployType deployType1, Language language1,
      String script2, DeployType deployType2, Language language2) {
    DeployedScript deployedScript1 = new DeployedScript(script1,
        deployType1, language1);
    DeployedScript deployedScript2 = new DeployedScript(script2,
        deployType2, language2);
    setScriptsToLoad(conf, deployedScript1, deployedScript2);
  }

  /**
   * Deploy scripts
   *
   * @param conf Configuration
   * @param scripts the scripts to deploy
   */
  public static void setScriptsToLoad(Configuration conf,
      DeployedScript... scripts) {
    List<DeployedScript> scriptsToLoad = Lists.newArrayList(scripts);
    SCRIPTS_TO_LOAD.set(conf, scriptsToLoad);
  }

  /**
   * Add a script to load on workers
   *
   * @param conf {@link Configuration}
   * @param script  Path to script
   * @param deployType type of deployment
   * @param language programming language
   */
  public static void addScriptToLoad(Configuration conf,
      String script, DeployType deployType, Language language) {
    addScriptToLoad(conf, new DeployedScript(script, deployType, language));
  }

  /**
   * Add a script to load on workers
   *
   * @param conf {@link Configuration}
   * @param script the script to load
   */
  public static void addScriptToLoad(Configuration conf,
      DeployedScript script) {
    List<DeployedScript> scriptsToLoad = getScriptsToLoad(conf);
    if (scriptsToLoad == null) {
      scriptsToLoad = Lists.<DeployedScript>newArrayList();
    }
    scriptsToLoad.add(script);
    SCRIPTS_TO_LOAD.set(conf, scriptsToLoad);
  }

  /**
   * Get the list of scripts to load on workers
   *
   * @param conf {@link Configuration}
   * @return list of {@link DeployedScript}s
   */
  public static List<DeployedScript> getScriptsToLoad(Configuration conf) {
    TypeReference<List<DeployedScript>> jsonType =
        new TypeReference<List<DeployedScript>>() { };
    return SCRIPTS_TO_LOAD.get(conf, jsonType);
  }

  /**
   * Load all the scripts deployed in Configuration
   *
   * @param conf Configuration
   * @throws IOException
   */
  public static void loadScripts(Configuration conf) throws IOException {
    List<DeployedScript> deployedScripts = getScriptsToLoad(conf);
    if (deployedScripts == null) {
      return;
    }
    for (DeployedScript deployedScript : deployedScripts) {
      loadScript(conf, deployedScript);
    }
  }

  /**
   * Load a single deployed script
   *
   * @param conf Configuration
   * @param deployedScript the deployed script
   * @throws IOException
   */
  public static void loadScript(Configuration conf,
      DeployedScript deployedScript) throws IOException {
    InputStream stream = openScriptInputStream(conf, deployedScript);
    switch (deployedScript.getLanguage()) {
    case JYTHON:
      loadJythonScript(stream);
      break;
    default:
      LOG.fatal("Don't know how to load script " + deployedScript);
      throw new IllegalStateException("Don't know how to load script " +
          deployedScript);
    }

    LOADED_SCRIPTS.add(deployedScript);
    Closeables.close(stream, true);
  }

  /**
   * Load a Jython deployed script
   *
   * @param stream InputStream with Jython code to load
   */
  private static void loadJythonScript(InputStream stream) {
    JythonUtils.getInterpreter().execfile(stream);
  }

  /**
   * Get list of scripts already loaded.
   *
   * @return list of loaded scripts
   */
  public static List<DeployedScript> getLoadedScripts() {
    return LOADED_SCRIPTS;
  }

  /**
   * Get an {@link java.io.InputStream} for the deployed script.
   *
   * @param conf Configuration
   * @param deployedScript the deployed script
   * @return {@link java.io.InputStream} for reading script
   */
  private static InputStream openScriptInputStream(Configuration conf,
      DeployedScript deployedScript) {
    DeployType deployType = deployedScript.getDeployType();
    String path = deployedScript.getPath();

    InputStream stream;
    switch (deployType) {
    case RESOURCE:
      if (LOG.isInfoEnabled()) {
        LOG.info("getScriptStream: Reading script from resource at " +
            deployedScript.getPath());
      }
      stream = ScriptLoader.class.getClassLoader().getResourceAsStream(path);
      if (stream == null) {
        throw new IllegalStateException("getScriptStream: Failed to " +
            "open script from resource at " + path);
      }
      break;
    case DISTRIBUTED_CACHE:
      if (LOG.isInfoEnabled()) {
        LOG.info("getScriptStream: Reading script from DistributedCache at " +
            path);
      }
      Optional<Path> localPath = getLocalCacheFile(conf, path);
      if (!localPath.isPresent()) {
        throw new IllegalStateException("getScriptStream: Failed to " +
            "find script in local DistributedCache matching " + path);
      }
      String pathStr = localPath.get().toString();
      try {
        stream = new BufferedInputStream(new FileInputStream(pathStr));
      } catch (IOException e) {
        throw new IllegalStateException("getScriptStream: Failed open " +
            "script from DistributedCache at " + localPath);
      }
      break;
    default:
      throw new IllegalArgumentException("getScriptStream: Unknown " +
          "script deployment type: " + deployType);
    }
    return stream;
  }
}
