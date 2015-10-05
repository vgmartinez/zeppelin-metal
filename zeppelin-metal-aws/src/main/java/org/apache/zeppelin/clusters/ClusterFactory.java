package org.apache.zeppelin.clusters;


import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.emr.EmrClusterFactory;
import org.apache.zeppelin.cluster.redshift.RedshiftClusterFactory;
import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author vgmartinez
 *
 */
public class ClusterFactory {
  static Logger logger = LoggerFactory.getLogger(ClusterFactory.class);
  
  public void createCluster(String name, int nodes, String instance, String type,
      Map<String, Boolean> apps) {
    if (type.equals("hadoop")) {
      Clusters cluster = (Clusters) new EmrClusterFactory();
      cluster.createCluster(name, nodes, instance, apps);
    } else if (type.equals("redshift")) {
      Clusters cluster = (Clusters) new RedshiftClusterFactory();
      cluster.createCluster(name, nodes, instance, null);
    }
  }
  
  public List<ClusterSetting> get() {
    ClusterImpl cluster = new ClusterImpl();
    return cluster.list();
  }
  
  public String getStatus(String clusterId) {
    ClusterImpl clusterImpl = new ClusterImpl();
    ClusterSetting cl = clusterImpl.get(clusterId);
    
    if (cl.getType().equals("hadoop")) {
      Clusters cluster = (Clusters) new EmrClusterFactory();
      return cluster.getStatus(clusterId);
    } else {
      Clusters cluster = (Clusters) new RedshiftClusterFactory();
      return cluster.getStatus(clusterId);
    }
  }
  
  public void remove(String clusterId) {
    ClusterImpl clusterImpl = new ClusterImpl();
    ClusterSetting cl = clusterImpl.get(clusterId);
    
    if (cl.getType().equals("hadoop")) {
      Clusters cluster = (Clusters) new EmrClusterFactory();
      cluster.remove(clusterId);
    } else if (cl.getType().equals("redshift")) {
      Clusters cluster = (Clusters) new RedshiftClusterFactory();
      cluster.remove(clusterId);
    }
  }
}
