/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.cluster.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.zeppelin.notebook.utility.IdHashes;

/**
 * Interpreter settings
 */
public class ClusterSetting {
  private String id;
  private String name;
  private int slaves;
  private String selected;
  private String status;
  private Map<String, String> urls = new HashMap<String, String>();
  private Map<String, Boolean> apps = new HashMap<String, Boolean>();
  private String type;
  
  public ClusterSetting(String id, String name, int slaves, String status, 
      Map<String, String> urls, String selected, String type, Map<String, Boolean> apps) {
    this.id = id;
    this.name = name;
    this.slaves = slaves;
    this.status = status;
    this.urls = urls;
    this.apps = apps;
    this.selected = selected;
    this.type = type;
    
  }
  
  public ClusterSetting(String name, int slaves, String status, Map<String, String> urls,
      String selected, String type, Map<String, Boolean> apps) {
    this(generateId(), name, slaves, status, urls, selected, type, apps);
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public String getId() {
    return id;
  }

  private static String generateId() {
    return IdHashes.encode(System.currentTimeMillis() + new Random().nextInt());
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  
  public Map<String, String> getUrl() {
    return urls;
  }

  public void setUrl(Map<String, String> urls) {
    this.urls = urls;
  }

  public void setSlaves(int memory) {
    this.slaves = slaves;
  }

  public int getSlaves() {
    return slaves;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getSelected() {
    return selected;
  }

  public void setSelected(String selected) {
    this.selected = selected;
  }

  public String getType() {
    return type;
  }

  public Map<String, Boolean> getApps() {
    return apps;
  }

  public void setApps(Map<String, Boolean> apps) {
    this.apps = apps;
  }

  public void setType(String type) {
    this.type = type;
  }
}
