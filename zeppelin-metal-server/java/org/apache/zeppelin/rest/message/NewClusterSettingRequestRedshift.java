package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestRedshift {
  String name;
  int slaves;
  String type;
  String passw;
  
  public NewClusterSettingRequestRedshift() {}

  public String getName() {
    return name;
  }

  public int getSlaves() {
    return slaves;
  }
  
  public String getType() {
    return type;
  }
}
