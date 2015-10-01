package org.apache.zeppelin.clusters;

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

public class ClusterImpl {
  static Logger logger = LoggerFactory.getLogger(Clusters.class);
  
  private Map<String, ClusterSetting> clusterSettings =
      new HashMap<String, ClusterSetting>();
  String[] clusterClassList;
  String sts, id = null;
  public static String clusterIdentifier = "";
  
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();
  
  
  public ClusterImpl() {
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
  
  public void add(ClusterSetting cluster) {
    clusterSettings.put(cluster.getId(), cluster);
    try {
      saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public ClusterSetting get(String clusterId) {
    return clusterSettings.get(clusterId);
  }
  
  public List<ClusterSetting> list() {
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
  
  public void remove(String clusterId) {
    clusterSettings.remove(clusterId);
    try {
      saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void saveToFile() throws IOException {
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
