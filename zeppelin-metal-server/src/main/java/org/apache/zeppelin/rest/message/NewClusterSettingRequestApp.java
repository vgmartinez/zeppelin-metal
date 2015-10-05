package org.apache.zeppelin.rest.message;

/**
 * 
 * @author vgmartinez
 *
 */

public class NewClusterSettingRequestApp {
  private boolean hive;
  private boolean spark;
  private boolean hue;
  
  public boolean getHive() {
    return hive;
  }
  public void setHive(boolean hive) {
    this.hive = hive;
  }
  public boolean getSpark() {
    return spark;
  }
  public void setSpark(boolean spark) {
    this.spark = spark;
  }
  public boolean getHue() {
    return hue;
  }
  public void setHue(boolean hue) {
    this.hue = hue;
  }
}
