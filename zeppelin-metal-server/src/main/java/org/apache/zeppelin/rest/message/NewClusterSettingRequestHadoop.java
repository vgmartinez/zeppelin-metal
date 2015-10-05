package org.apache.zeppelin.rest.message;

import java.util.HashMap;
import java.util.Map;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestHadoop {
  String name;
  int slaves;
  String instance;
  NewClusterSettingRequestApp app;
  
  public NewClusterSettingRequestHadoop() {}

  public String getName() {
    return name;
  }

  public int getSlaves() {
    return slaves;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  public Map<String, Boolean> getApp() {
    Map<String, Boolean> apps = new HashMap<String, Boolean>();
    apps.put("hive", app.getHive());
    apps.put("spark", app.getSpark());
    apps.put("hue", app.getHue());
    return apps;
  }

  public void setApp(NewClusterSettingRequestApp app) {
    this.app = app;
  }
}
