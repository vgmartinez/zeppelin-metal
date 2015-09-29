package org.apache.zeppelin.rest.message;

/**
 *  NewClusterSetting rest api request message
 *
 */
public class NewClusterSettingRequestSpark {
  String name;
  int slaves;

  public NewClusterSettingRequestSpark() {}

  public String getName() {
    return name;
  }

  public int getSlaves() {
    return slaves;
  }
}
