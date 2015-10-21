package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */

public class NewClusterSettingRequestRds {
  String name;
  String passw;
  String user;
  String instance;
  String engine;
  int storage;
  String version;

  public NewClusterSettingRequestRds() {}

  public String getName() {
    return name;
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
  
  public String getEngine() {
    return engine;
  }
  
  public int getStorage() {
    return storage;
  }
  
  public String getVersion() {
    return version;
  }
}
