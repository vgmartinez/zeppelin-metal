package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestHadoop {
  String name;
  int slaves;
  
  public NewClusterSettingRequestHadoop() {}

  public String getName() {
    return name;
  }

  public int getSlaves() {
    return slaves;
  }
}
