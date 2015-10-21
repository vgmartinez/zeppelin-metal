package org.apache.zeppelin.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.emr.ClusterSettingEmr;
import org.apache.zeppelin.cluster.emr.EmrClusterFactory;
import org.apache.zeppelin.cluster.rds.ClusterSettingRds;
import org.apache.zeppelin.cluster.rds.RdsClusterFactory;
import org.apache.zeppelin.cluster.redshift.ClusterSettingRedshift;
import org.apache.zeppelin.cluster.redshift.RedshiftClusterFactory;
import org.apache.zeppelin.cluster.utils.ClusterInfoSaving;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * @author vgmartinez
 *
 */
public class ClusterFactory {
  static Logger logger = LoggerFactory.getLogger(ClusterFactory.class);
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();
  private Map<String, ClusterSettingEmr> EMRCluster =
      new HashMap<String, ClusterSettingEmr>();
  private Map<String, ClusterSettingRedshift> RedshiftCluster =
      new HashMap<String, ClusterSettingRedshift>();
  private Map<String, ClusterSettingRds> RDSCluster =
      new HashMap<String, ClusterSettingRds>();
  
  RedshiftClusterFactory redshift = new RedshiftClusterFactory();
  EmrClusterFactory emr = new EmrClusterFactory();
  RdsClusterFactory rds = new RdsClusterFactory();
  
  public ClusterFactory() {
    try {
      loadFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    GsonBuilder builder = new GsonBuilder();
    builder.setPrettyPrinting();
    builder.registerTypeAdapter(Interpreter.class, new InterpreterSerializer());
    gson = builder.create();
  }
  
  public void createCluster(String name, String instanceType, String type,
      Object ... args) {
    if (type.equals("emr")) {
      ClusterSettingEmr cls = emr.createCluster(name, instanceType, (int) args[0], 
          (Map<String, Boolean>) args[1]);
      EMRCluster.put(cls.getId(), cls);
      
    } else if (type.equals("redshift")) {
      ClusterSettingRedshift cls = redshift.createCluster(name, instanceType, (int) args[0], 
          (String) args[1], (String) args[2]);
      RedshiftCluster.put(cls.getId(), cls);
      
    } else if (type.equals("rds")) {
      ClusterSettingRds cls = rds.createCluster(name, instanceType, (int) args[0],
          (String) args[1], (String) args[2], (String) args[3], (String) args[4]);
      RDSCluster.put(cls.getId(), cls);
    }
    saveToFile();
  }
  
  public List<Object> getStatus() {
    List<Object> orderedSettings = new LinkedList<Object>();
    
    for (ClusterSettingEmr cl: EMRCluster.values()) {
      String clusterId = cl.getId();
      orderedSettings.add(cl);
      
      String status = emr.getStatus(clusterId);
      Map<String, String> urls = new HashMap<String, String>();
      
      if (status.equals("deleting")) {
        EMRCluster.remove(clusterId);
      } else if (status.equals("running")){
        String dns = emr.getDnsMaster(clusterId);          
        if (dns != null && !dns.isEmpty()) {
          logger.info("MASTER: " + dns);
          if (cl.getApps().get("hue")) {
            urls.put("hue", "http://" + dns + ":8888");          
          }
          urls.put("master", "http://" + dns + ":8088");
          urls.put("dns", dns);
          cl.setUrl(urls);
        }
      }
      cl.setStatus(status);
    }
    
    for (ClusterSettingRedshift cl: RedshiftCluster.values()) {
      String clusterId = cl.getId();
      orderedSettings.add(cl);
      
      String status = redshift.getStatus(RedshiftCluster
          .get(clusterId).getName());
      Map<String, String> urls = new HashMap<String, String>();
      
      if (status.equals("deleting")) {
        RedshiftCluster.remove(clusterId);
      }
      else if (status.equals("running")) {
        String dns = redshift.getUrlRedshift(cl.getName());
        if (dns != null && !dns.isEmpty()) {
          urls.put("dns", dns);
          cl.setUrl(urls);
        }
      }
      cl.setStatus(status);
    }
    
    for (ClusterSettingRds cl: RDSCluster.values()) {
      String clusterId = cl.getId();
      orderedSettings.add(cl);
      
      String status = rds.getStatus(RDSCluster
          .get(clusterId).getName());
      Map<String, String> urls = new HashMap<String, String>();
      
      if (status.equals("deleting")) {
        RDSCluster.remove(clusterId);
      }
      else if (status.equals("running")) {
        String endpoint = rds.getEndpoint(cl.getName());
        if (endpoint != null && !endpoint.isEmpty()) {
          urls.put("dns", endpoint + ":3306");
          cl.setUrl(urls);
        }
      }
      cl.setStatus(status);
    }
    
    saveToFile();
    return orderedSettings;
  }
  
  public void remove(String clusterType, String clusterId) {
    switch (clusterType) {
        case "emr":
          emr.remove(clusterId);
          EMRCluster.remove(clusterId);
          break;
        case "redshift":
          String nameRedshift = RedshiftCluster.get(clusterId).getName();
          redshift.remove(nameRedshift);
          RedshiftCluster.remove(clusterId);
          break;
        case "rds":
          String nameRDS = RDSCluster.get(clusterId).getName();
          rds.remove(nameRDS);
          RDSCluster.remove(clusterId);
          break;
    }
    saveToFile();
  }
  
  public void setClusterToInterpreter(String intId, String clustId) {
    /*if (clustId.equals("")) {
      for (ClusterSettingEmr setting : clusterSettings.values()) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      } 
    } else {
      for (ClusterSettingEmr setting : clusterSettings.values()) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      }
      clusterSettings.get(clustId).setSelected(intId);
    }*/
  }
  
  public void saveToFile() {
    String jsonString;

    ClusterInfoSaving info = new ClusterInfoSaving();
    info.EMR = EMRCluster;
    info.Redshift = RedshiftCluster;
    info.RDS = RDSCluster;

    jsonString = gson.toJson(info);
    
    try {
      File settingFile = new File(conf.getConfDir() + "/clusters.json");
      if (!settingFile.exists()) {
        settingFile.createNewFile();
      }
  
      FileOutputStream fos = new FileOutputStream(settingFile, false);
      OutputStreamWriter out = new OutputStreamWriter(fos);
      out.append(jsonString);
      out.close();
      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
    for (String k : info.EMR.keySet()) {
      ClusterSettingEmr setting = info.EMR.get(k);

      ClusterSettingEmr clustSetting = new ClusterSettingEmr(
        setting.getId(),
        setting.getName(),
        setting.getSlaves(),
        setting.getStatus(),
        setting.getUrl(),
        setting.getSelected(),
        setting.getType(),
        setting.getInstanceType(),
        setting.getApps());

      EMRCluster.put(k, clustSetting);
    }
    for (String k : info.Redshift.keySet()) {
      ClusterSettingRedshift setting = info.Redshift.get(k);

      ClusterSettingRedshift clustSetting = new ClusterSettingRedshift(
        setting.getId(),
        setting.getName(),
        setting.getSlaves(),
        setting.getStatus(),
        setting.getUrl(),
        setting.getSelected(),
        setting.getType(),
        setting.getInstanceType(),
        setting.getApps());

      RedshiftCluster.put(k, clustSetting);
    }
    for (String k : info.RDS.keySet()) {
      ClusterSettingRds setting = info.RDS.get(k);

      ClusterSettingRds clustSetting = new ClusterSettingRds(
        setting.getId(),
        setting.getName(),
        setting.getUser(),
        setting.getPassw(),
        setting.getStatus(),
        setting.getUrl(),
        setting.getType(),
        setting.getInstanceType(),
        setting.getSelected(),
        setting.getEngine(),
        setting.getStorage(),
        setting.getVersion());

      RDSCluster.put(k, clustSetting);
    }
  }
}
