/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * The ListResultSchema model for DFS Endpoint Listing.
 */
@InterfaceStability.Evolving
@JsonIgnoreProperties(ignoreUnknown = true)
public class DfsListResultSchema implements ListResultSchema {
  /**
   * List of paths returned by DFS Endpoint Listing.
   */
  @JsonProperty(value = "paths")
  private List<DfsListResultEntrySchema> paths;

  /**
   * Get the list of paths returned by DFS Endpoint Listing.
   * @return the paths value
   */
  public List<DfsListResultEntrySchema> paths() {
    return this.paths;
  }

  /**
   * Set the paths value to list of paths returned by DFS Endpoint Listing.
   * @param paths the paths value to set
   * @return the ListSchema object itself.
   */
  public ListResultSchema withPaths(final List<? extends ListResultEntrySchema> paths) {
    this.paths = (List<DfsListResultEntrySchema>) paths;
    return this;
  }
}
