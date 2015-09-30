package org.apache.zeppelin.cluster.redshift;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.utils.ClusterInfoSaving;
import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DeleteClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * Interpreter Rest API
 *
 */
public class RedshiftClusterFactory {
  static Logger logger = LoggerFactory.getLogger(RedshiftClusterFactory.class);
  
  private Map<String, ClusterSetting> clusterSettings =
      new HashMap<String, ClusterSetting>();
  String[] clusterClassList;
  String sts, id = null;
  public static String clusterIdentifier = "";
  
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();
  
  public static AmazonRedshiftClient client = new AmazonRedshiftClient(
		  new DefaultAWSCredentialsProviderChain());
  
  public RedshiftClusterFactory() {
    try {
      loadFromFile();
    } catch (IOException e) {
      e.printStackTrace();
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

  private String getStatusRedshift(String clusterId) throws InterruptedException {
    clusterIdentifier = clusterSettings.get(clusterId).getName();
    DescribeClustersResult result = client.describeClusters(new DescribeClustersRequest()
        .withClusterIdentifier(clusterIdentifier));
    String status = (result.getClusters()).get(0).getClusterStatus();
    if (status.equalsIgnoreCase("available")) {
      clusterSettings.get(clusterId).setUrl(getUrlRedshift(clusterId));
      try {
        saveToFile();
      } catch (IOException e) {
        e.printStackTrace();
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
  
  public List<ClusterSetting> getStatus() {
    String status = null;
    for (ClusterSetting cluster: clusterSettings.values()) {
      try {
        status = getStatusRedshift(cluster.getId());
        cluster.setStatus(status);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return new ArrayList<ClusterSetting>(clusterSettings.values());
  }
  
  private void loadFromFile() throws IOException {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.create();
    File settingFile = new File(conf.getConfDir() + "/clusters.json");
    if (!settingFile.exists()) {
      return;
    }
    FileInputStream fis = new FileInputStream(settingFile);
    InputStreamReader isr = new InputStreamReader(fis);
    BufferedReader bufferedReader = new BufferedReader(isr);
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      sb.append(line);
    }
    isr.close();
    fis.close();

    String json = sb.toString();
    ClusterInfoSaving info = gson.fromJson(json, ClusterInfoSaving.class);
    for (String k : info.clusterSettings.keySet()) {
      ClusterSetting setting = info.clusterSettings.get(k);

      ClusterSetting clustSetting = new ClusterSetting(
        setting.getId(),
        setting.getName(),
        setting.getSlaves(),
        setting.getStatus(),
        setting.getUrl(),
        setting.getSelected(),
        setting.getType());

      clusterSettings.put(k, clustSetting);
    }
  }

  private void saveToFile() throws IOException {
    String jsonString;

    synchronized (clusterSettings) {
      ClusterInfoSaving info = new ClusterInfoSaving();
      info.clusterSettings = clusterSettings;

      jsonString = gson.toJson(info);
    }

    File settingFile = new File(conf.getConfDir() + "/clusters.json");
    if (!settingFile.exists()) {
      settingFile.createNewFile();
    }

    FileOutputStream fos = new FileOutputStream(settingFile, false);
    OutputStreamWriter out = new OutputStreamWriter(fos);
    out.append(jsonString);
    out.close();
    fos.close();
  }
  
  /**
   * Get loaded interpreters
   * @return
   */
  public List<ClusterSetting> get() {
    synchronized (clusterSettings) {
      List<ClusterSetting> orderedSettings = new LinkedList<ClusterSetting>();
      List<ClusterSetting> settings = new LinkedList<ClusterSetting>(
          clusterSettings.values());
      
      for (ClusterSetting setting : settings) {
        orderedSettings.add(setting); 
      }
      return orderedSettings;
    }
  }
  
  /**
   * @param name user defined name
   * @param slaves 
   * @throws IOException
   */
  public boolean addRedshift(String name, int slaves, String type)
      throws IOException {

    ClusterSetting clustSetting = new ClusterSetting(id, name, slaves,
        "starting", "", "", "redshift");
    id = clustSetting.getId();
    
    if (createClusterRedshift(name, slaves, "admin", "Admin123", type)){
      clusterSettings.put(id, clustSetting);
      saveToFile();
      return true;
    } else {
      return false;
    }
  }
  
  public boolean remove(String clusterId, boolean snapshot)
      throws IOException {
    return removeRedshiftCluster(clusterId, snapshot);
  }
  
  public boolean removeRedshiftCluster(String clusterId, boolean snapshot) {
    String name = clusterSettings.get(clusterId).getName();
    if (snapshot) {
      client.deleteCluster(new DeleteClusterRequest()
          .withClusterIdentifier(name)
          .withFinalClusterSnapshotIdentifier(name)); 
    } else {
      client.deleteCluster(new DeleteClusterRequest()
          .withClusterIdentifier(name)
          .withSkipFinalClusterSnapshot(true));
    }
    clusterSettings.remove(clusterId);
    try {
      saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }
  
  public void setClusterToInterpreter(String intId, String clustId) {
    List<ClusterSetting> settings = new LinkedList<ClusterSetting>(
        clusterSettings.values());
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
      clusterSettings.get(clustId).setSelected(intId);
    }
    try {
      saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
