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

package org.apache.zeppelin.cluster.rds;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.zeppelin.notebook.utility.IdHashes;

/**
 * Cluster settings
 */
public class ClusterSettingRds {
  private String id;
  private String name;
  private String user;
  private String passw;
  private String selected;
  private String status;
  private Map<String, String> urls = new HashMap<String, String>();
  private String type;
  private String instanceType;
  private String engine;
  private int storage;
  private String version;
  
  public ClusterSettingRds(String id, String name, String user, String passw, String status, 
      Map<String, String> urls, String type, String instanceType, String selected, 
      String engine, int storage, String version) {
    this.id = id;
    this.name = name;
    this.user = user;
    this.passw = passw;
    this.status = status;
    this.urls = urls;
    this.selected = selected;
    this.type = type;
    this.instanceType = instanceType;
    this.engine = engine;
    this.storage = storage;
    this.version = version;
  }
  
  public ClusterSettingRds(String name, String user, String passw, String status, 
      Map<String, String> urls, String selected, String type, String instanceType,
      String engine, int storage, String version) {
    this(generateId(), name, user, passw, status, urls, type, instanceType, selected, 
        engine, storage, version);
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
  
  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassw() {
    return passw;
  }

  public void setPassw(String passw) {
    this.passw = passw;
  }

  public Map<String, String> getUrl() {
    return urls;
  }

  public void setUrl(Map<String, String> urls) {
    this.urls = urls;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  
  public String getInstanceType() {
    return instanceType;
  }

  public void setInstanceType(String instanceType) {
    this.instanceType = instanceType;
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

  public String getEngine() {
    return engine;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }

  public int getStorage() {
    return storage;
  }

  public void setStorage(int storage) {
    this.storage = storage;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
