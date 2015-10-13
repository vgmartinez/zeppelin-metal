package org.apache.zeppelin.clusters;

import java.io.IOException;
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
      ClusterSetting cls = cluster.createCluster(name, nodes, instance, 
          (Map<String, Boolean>) args[0]);
      clusterImpl.add(cls);
    } else if (type.equals("redshift")) {
      RedshiftClusterFactory cluster = new RedshiftClusterFactory();
      ClusterSetting cls = cluster.createCluster(name, nodes, instance, 
          (String) args[1], (String) args[2]);
      clusterImpl.add(cls);
    }
  }
  
  public List<ClusterSetting> get() {
    return clusterImpl.list();
  }
  
  public List<ClusterSetting> getStatus() {
    List<ClusterSetting> clusterSettings = clusterImpl.list();
    String status = null;
    for (ClusterSetting cl: clusterSettings) {
      String clusterId = cl.getId();
      if (cl.getType().equals("emr")) {
        EmrClusterFactory cluster = new EmrClusterFactory();
        status = cluster.getStatusEmr(clusterId);
        logger.info("Status EMR: " + status);
        if (status.contains("TERMINATED")) {
          cl.setStatus(status);
          logger.info("eliminando emr");
          clusterImpl.remove(clusterId);
        } else {
          cl.setStatus(status);
        } 
      } else {
        RedshiftClusterFactory cluster = new RedshiftClusterFactory();
        status = cluster.getStatus(clusterId);
        logger.info("Status REDSHIFT: " + status);
        if (status.contains("DELETING")) {
          cl.setStatus(status);
          logger.info("eliminando redshift");
          clusterImpl.remove(clusterId);
        } else {
          cl.setStatus(status);
        }
      }
    }
    return clusterImpl.list();
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
    try {
      clusterImpl.saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
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
