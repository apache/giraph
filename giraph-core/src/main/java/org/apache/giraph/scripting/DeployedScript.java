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

import com.google.common.base.MoreObjects;
import org.apache.giraph.graph.Language;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.base.Objects;

/**
 * A script that was deployed to the cluster.
 */
public class DeployedScript {
  /** How the script was deployed */
  @JsonProperty
  private final DeployType deployType;
  /** Path to the script */
  @JsonProperty
  private final String path;
  /** Programming language the script is written in */
  @JsonProperty
  private final Language language;

  /**
   * Constructor
   *
   * @param path String path to resource
   * @param deployType deployment type
   * @param language programming language
   */
  @JsonCreator
  public DeployedScript(
      @JsonProperty("path") String path,
      @JsonProperty("deployType") DeployType deployType,
      @JsonProperty("language") Language language) {
    this.path = path;
    this.deployType = deployType;
    this.language = language;
  }

  public DeployType getDeployType() {
    return deployType;
  }

  public String getPath() {
    return path;
  }

  public Language getLanguage() {
    return language;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(path, deployType, language);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DeployedScript) {
      DeployedScript other = (DeployedScript) obj;
      return Objects.equal(path, other.path) &&
          Objects.equal(deployType, other.deployType) &&
          Objects.equal(language, other.language);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("deployType", deployType)
        .add("language", language)
        .toString();
  }
}
