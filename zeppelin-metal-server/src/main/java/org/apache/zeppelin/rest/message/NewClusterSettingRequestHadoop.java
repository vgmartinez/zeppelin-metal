package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestHadoop {
  String name;
  int slaves;
  String instance;
  
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
  
}
