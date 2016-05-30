package org.apache.zeppelin.cluster;

import java.util.List;

public interface Cluster {
  public void create(ClusterSetting cluster);
  public List<Object> getStatus();
  public void remove();
}