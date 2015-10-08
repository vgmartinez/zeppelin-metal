package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestRedshift {
  String name;
  int slaves;
  String instance;
  String passw;
  String user;
  
  public NewClusterSettingRequestRedshift() {}

  public String getName() {
    return name;
  }

  public int getSlaves() {
    return slaves;
  }
  
  public String getUser() {
    return user;
  }
  
  public String getPassw() {
    return passw;
  }
  
  public String getInstance() {
    return instance;
  }
}
