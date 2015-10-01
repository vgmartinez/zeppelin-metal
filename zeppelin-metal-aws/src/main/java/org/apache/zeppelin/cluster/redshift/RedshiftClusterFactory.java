package org.apache.zeppelin.cluster.redshift;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.apache.zeppelin.clusters.ClusterImpl;
import org.apache.zeppelin.clusters.Clusters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DeleteClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;


/**
 * Interpreter Rest API
 *
 */
public class RedshiftClusterFactory extends Clusters {
  static Logger logger = LoggerFactory.getLogger(RedshiftClusterFactory.class);
  
  String sts, id = null;
  public static String clusterIdentifier = "";
  
  public static AmazonRedshiftClient client = new AmazonRedshiftClient(
      new DefaultAWSCredentialsProviderChain());
  
  ClusterImpl clusterImpl = new ClusterImpl();
  
  public RedshiftClusterFactory() {}

  @Override
  public void createCluster(String name, int slaves, String type) {

    ClusterSetting clustSetting = new ClusterSetting(name, slaves,
        "starting", null, "", "redshift");
    
    if (createClusterRedshift(name, slaves, "admin", "Admin123", type)){
      clusterImpl.add(clustSetting);
    }
  }
  
  public boolean createClusterRedshift(String name, int slaves, 
      String user, String passw, String type) {
    clusterIdentifier = name;
    try {            
      CreateClusterRequest request = new CreateClusterRequest()
          .withClusterIdentifier(name)
          .withMasterUsername(user)
          .withMasterUserPassword(passw)
          .withNodeType(type)
          .withNumberOfNodes(slaves);          
        
      Cluster createResponse = client.createCluster(request);
      logger.info("Created cluster " + createResponse.getClusterIdentifier());
      
    } catch (Exception e) {
      logger.info("Operation failed: " + e.getMessage());
      return false;
    }
    return true;
  }

  private String getStatusRedshift(String clusterId) {
    Map<String, String> urls = new HashMap<String, String>();
    String status = null;
    clusterIdentifier = clusterImpl.get(clusterId).getName();
    DescribeClustersResult result = client.describeClusters(new DescribeClustersRequest()
        .withClusterIdentifier(clusterIdentifier));
    
    List<Cluster> Redshiftclusters = result.getClusters();
    for (Cluster cluster: Redshiftclusters) {
      if (cluster.getClusterIdentifier() == clusterIdentifier) {
        status = cluster.getClusterStatus();
      }
    }
    if (status.equalsIgnoreCase("available")) {
      String dns = getUrlRedshift(clusterId);
      if (!dns.isEmpty()) {
        urls.put("host", dns);
        clusterImpl.get(clusterId).setUrl(urls);
      }
      return status;
    }
    return status;
  }
  
  private String getUrlRedshift(String clusterId) {
    DescribeClustersResult result = client.describeClusters(new DescribeClustersRequest()
        .withClusterIdentifier(clusterIdentifier));
    String url = result.getClusters().get(0).getEndpoint().getAddress();
    return url;
  }
  
  public String getStatus(String clusterId) {
    ClusterSetting cluster = clusterImpl.get(clusterId);
    
    String status = getStatusRedshift(clusterId);
    
    cluster.setStatus(status);
    
    return status;
  }
  
  public boolean remove(String clusterId, boolean snapshot) {
    clusterImpl.remove(clusterId);
    return removeRedshiftCluster(clusterId, snapshot);
  }
  
  public boolean removeRedshiftCluster(String clusterId, boolean snapshot) {
    String name = clusterImpl.get(clusterId).getName();
    if (snapshot) {
      client.deleteCluster(new DeleteClusterRequest()
          .withClusterIdentifier(name)
          .withFinalClusterSnapshotIdentifier(name)); 
    } else {
      client.deleteCluster(new DeleteClusterRequest()
          .withClusterIdentifier(name)
          .withSkipFinalClusterSnapshot(true));
    }
    return true;
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
