package org.apache.zeppelin.clusters;


import java.util.LinkedList;
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
  ClusterImpl clusterImpl = new ClusterImpl();
  
  public void createCluster(String name, int nodes, String instance, String type,
      Object ... args) {
    if (type.equals("emr")) {
      EmrClusterFactory cluster = new EmrClusterFactory();
      cluster.createCluster(name, nodes, instance, (Map<String, Boolean>) args[0]);
    } else if (type.equals("redshift")) {
      RedshiftClusterFactory cluster = new RedshiftClusterFactory();
      cluster.createCluster(name, nodes, instance, (String) args[1], (String) args[2]);
    }
  }
  
  public List<ClusterSetting> get() {
    return clusterImpl.list();
  }
  
  public String getStatus(String clusterId) {
    ClusterSetting cl = clusterImpl.get(clusterId);
    
    if (cl.getType().equals("emr")) {
      EmrClusterFactory cluster = new EmrClusterFactory();
      return cluster.getStatus(clusterId);
    } else {
      RedshiftClusterFactory cluster = new RedshiftClusterFactory();
      return cluster.getStatus(clusterId);
    }
  }
  
  public void remove(String clusterId) {
    ClusterSetting cl = clusterImpl.get(clusterId);
    
    if (cl.getType().equals("emr")) {
      EmrClusterFactory cluster = new EmrClusterFactory();
      cluster.remove(clusterId);
    } else if (cl.getType().equals("redshift")) {
      RedshiftClusterFactory cluster = new RedshiftClusterFactory();
      cluster.remove(clusterId);
    }
  }
  
  public void setClusterToInterpreter(String intId, String clustId) {
    List<ClusterSetting> settings = new LinkedList<ClusterSetting>(clusterImpl.list());
    if (clustId.equals("")) {
      for (ClusterSetting setting : settings) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      } 
    } else {
      for (ClusterSetting setting : settings) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      }
      clusterImpl.get(clustId).setSelected(intId);
    }
  }
}
