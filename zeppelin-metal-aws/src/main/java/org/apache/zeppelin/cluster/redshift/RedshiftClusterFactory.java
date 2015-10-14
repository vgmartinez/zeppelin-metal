package org.apache.zeppelin.cluster.redshift;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.apache.zeppelin.clusters.ClusterImpl;
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
public class RedshiftClusterFactory {
  static Logger logger = LoggerFactory.getLogger(RedshiftClusterFactory.class);
  
  public static String clusterIdentifier = "";
  
  public static AmazonRedshiftClient client = new AmazonRedshiftClient(
      new DefaultAWSCredentialsProviderChain());
  
  ClusterImpl clusterImpl = new ClusterImpl();
  
  public RedshiftClusterFactory() {}

  public ClusterSetting createCluster(String name, int slaves, 
      String type, String user, String passw) {
    ClusterSetting clustSetting = new ClusterSetting(name, slaves,
        "starting", null, "", "redshift", null);
    createClusterRedshift(name, slaves, user, passw, type);
    
    return clustSetting;
  }
  
  public void createClusterRedshift(String name, int slaves, 
      String user, String passw, String type) {
    clusterIdentifier = name;
    CreateClusterRequest request = new CreateClusterRequest()
        .withClusterIdentifier(name)
        .withMasterUsername(user)
        .withMasterUserPassword(passw)
        .withNodeType(type)
        .withNumberOfNodes(slaves);          
        
    Cluster createResponse = client.createCluster(request);
    logger.info("Created cluster " + createResponse.getClusterIdentifier());
  }

  private String getStatusRedshift(String clusterId) {
    Map<String, String> urls = new HashMap<String, String>();
    String status = null;
    clusterIdentifier = clusterImpl.get(clusterId).getName();
    DescribeClustersResult result = client.describeClusters(new DescribeClustersRequest()
        .withClusterIdentifier(clusterIdentifier));
    List<Cluster> Redshiftclusters = result.getClusters();
    for (Cluster cluster: Redshiftclusters) {
      if (cluster.getClusterIdentifier().equals(clusterIdentifier)) {
        status = cluster.getClusterStatus();
      }
    }
    if (status.equalsIgnoreCase("available")) {
      String dns = getUrlRedshift(clusterId);
      if (dns != null && !dns.isEmpty()) {
        urls.put("dns", dns);
        clusterImpl.get(clusterId).setUrl(urls);
      }
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
    String status;
    switch (getStatusRedshift(clusterId)) {
        case "available":
          status = "running";
          break;
        case "creating":
          status = "starting";
          break;
        case "deleting":
          status = "deleting";
          break;
        default:
          status = "failed";
          break;
    }
    return status;
  }
  
  public void remove(String clusterId) {
    removeRedshiftCluster(clusterId);
  }
  
  public void removeRedshiftCluster(String clusterId) {
    String name = clusterImpl.get(clusterId).getName();
    client.deleteCluster(new DeleteClusterRequest()
        .withClusterIdentifier(name)
        .withSkipFinalClusterSnapshot(true));
  }
}
