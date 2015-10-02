package org.apache.zeppelin.clusters;


/**
 * Clusters abstract class
 *
 */
public abstract class Clusters {
  
  public abstract void createCluster(String name, int nodes, String type);
  
  public abstract String getStatus(String clusterId);
  
  public abstract void remove(String clusterId);
}
