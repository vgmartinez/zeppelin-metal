package org.apache.zeppelin.clusters;

import java.util.Map;


/**
 * Clusters abstract class
 *
 */
public interface Clusters {
  
  public abstract void createCluster(String name, int nodes, 
      String type, Map<String, Boolean> apps);
  
  public abstract String getStatus(String clusterId);
  
  public abstract void remove(String clusterId);
}
