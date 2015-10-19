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

import org.apache.zeppelin.cluster.emr.EmrClusterFactory;
import org.apache.zeppelin.cluster.redshift.RedshiftClusterFactory;
import org.apache.zeppelin.cluster.utils.ClusterInfoSaving;
import org.apache.zeppelin.cluster.utils.ClusterSetting;
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
  private Map<String, ClusterSetting> clusterSettings =
      new HashMap<String, ClusterSetting>();
  
  
  RedshiftClusterFactory redshift = new RedshiftClusterFactory();
  EmrClusterFactory emr = new EmrClusterFactory();
  
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
  
  public void createCluster(String name, int nodes, String instance, String type,
      Object ... args) {
    if (type.equals("emr")) {
      ClusterSetting cls = emr.createCluster(name, nodes, instance, 
          (Map<String, Boolean>) args[0]);
      clusterSettings.put(cls.getId(), cls);
      
    } else if (type.equals("redshift")) {
      ClusterSetting cls = redshift.createCluster(name, nodes, instance, 
          (String) args[1], (String) args[2]);
      clusterSettings.put(cls.getId(), cls);
    }
  }
  
  public List<ClusterSetting> getStatus() {
    List<ClusterSetting> orderedSettings = new LinkedList<ClusterSetting>();
    
    for (ClusterSetting cl: clusterSettings.values()) {
      String clusterId = cl.getId();
      orderedSettings.add(cl); 
      if (cl.getType().equals("emr")) {
        String status = emr.getStatus(clusterId);
        Map<String, String> urls = new HashMap<String, String>();
        
        if (status.equals("deleting")) {
          clusterSettings.remove(clusterId);
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
      } else {
        String status = redshift.getStatus(clusterSettings
            .get(clusterId).getName());
        Map<String, String> urls = new HashMap<String, String>();
        
        if (status.equals("deleting")) {
          clusterSettings.remove(clusterId);
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
    }
    saveToFile();
    return orderedSettings;
  }
  
  public void remove(String clusterId) {
    ClusterSetting cl = clusterSettings.get(clusterId);
    
    if (cl.getType().equals("emr")) {
      emr.remove(clusterId);
    } else if (cl.getType().equals("redshift")) {
      String name = clusterSettings.get(clusterId)
          .getName();
      redshift.remove(name);
    }
    clusterSettings.remove(clusterId);
    saveToFile();
  }
  
  public void setClusterToInterpreter(String intId, String clustId) {
    List<ClusterSetting> settings = new LinkedList<ClusterSetting>();
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
  }
  
  public void saveToFile() {
    String jsonString;

    synchronized (clusterSettings) {
      ClusterInfoSaving info = new ClusterInfoSaving();
      info.clusterSettings = clusterSettings;

      jsonString = gson.toJson(info);
    }
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
    for (String k : info.clusterSettings.keySet()) {
      ClusterSetting setting = info.clusterSettings.get(k);

      ClusterSetting clustSetting = new ClusterSetting(
        setting.getId(),
        setting.getName(),
        setting.getSlaves(),
        setting.getStatus(),
        setting.getUrl(),
        setting.getSelected(),
        setting.getType(),
        setting.getApps());

      clusterSettings.put(k, clustSetting);
    }
  }
}
