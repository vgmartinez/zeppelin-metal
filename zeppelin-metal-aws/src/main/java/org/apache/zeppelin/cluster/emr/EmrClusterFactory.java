package org.apache.zeppelin.cluster.emr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.SupportedProductConfig;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * Interpreter Rest API
 *
 */
public class EmrClusterFactory {
  static Logger logger = LoggerFactory.getLogger(EmrClusterFactory.class);
  
  private Map<String, ClusterSetting> clusterSettings =
      new HashMap<String, ClusterSetting>();
  String[] clusterClassList;
  String sts, id = null;
  private RunJobFlowResult result;
  public static String clusterIdentifier = "";
  
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();
  
  AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(
		  new DefaultAWSCredentialsProviderChain());
  
  static AmazonEC2 ec2 = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
  
  
  public EmrClusterFactory() {
    try {
      loadFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void addSpark(String name, int slaves)
      throws IOException {

    createClusterSpark(name, slaves);
    ClusterSetting clustSetting = new ClusterSetting(name, slaves, "starting", "", "", "spark");
    clusterSettings.put(clustSetting.getId(), clustSetting);
    saveToFile();
  }
  
  public String createClusterSpark(String name, int slaves){
    
    Application sparkApp = new Application()
      .withName("Spark");
    
    List<Application> myApps = new ArrayList<Application>();
    myApps.add(sparkApp);

    RunJobFlowRequest request = new RunJobFlowRequest()
        .withName(name)
        .withReleaseLabel("emr-4.0.0")
        .withApplications(myApps)
        .withServiceRole("EMR_DefaultRole")
        .withJobFlowRole("EMR_EC2_DefaultRole")
        .withInstances(new JobFlowInstancesConfig()
            .withInstanceCount(slaves + 1)
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withMasterInstanceType("m1.medium")
            .withSlaveInstanceType("m1.medium")
      );
    result = emr.runJobFlow(request);
    return result.getJobFlowId();
  }
  
  public void addHadoop(String name, int slaves)
      throws IOException {
    
    String id = createClusterHadoop(name, slaves);
    ClusterSetting clustSetting = new ClusterSetting(id, name, slaves, 
        "starting", "", "", "hadoop");
    clusterSettings.put(id, clustSetting);
    saveToFile();
  }
  
  public String createClusterHadoop(String name, int slaves){
    
    StepFactory stepFactory = new StepFactory();
    
    StepConfig enabledebugging = new StepConfig()
        .withName("Enable debugging")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

    StepConfig installHive = new StepConfig()
        .withName("Install Hive")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newInstallHiveStep());
    
    HadoopJarStepConfig hadoopStep = new HadoopJarStepConfig()
        .withJar("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
        .withArgs("s3://elasticmapreduce/libs/hue/run-hue");
    
    StepConfig installHue = new StepConfig()
        .withName("Install Hue")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(hadoopStep);
    
    SupportedProductConfig sparkConfig = new SupportedProductConfig()
        .withName("Spark");
    
    BootstrapActionConfig boots = new BootstrapActionConfig("Install Hue",
            new ScriptBootstrapActionConfig("s3://elasticmapreduce/libs/hue/install-hue", null));
    
    RunJobFlowRequest request = new RunJobFlowRequest()
        .withName(name)
        .withAmiVersion("3.8.0")
        .withSteps(enabledebugging, installHive, installHue)
        .withNewSupportedProducts(sparkConfig)
        .withBootstrapActions(boots)
        .withServiceRole("EMR_DefaultRole")
        .withJobFlowRole("EMR_EC2_DefaultRole")
        .withInstances(new JobFlowInstancesConfig()
            .withInstanceCount(slaves)
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withMasterInstanceType("m1.medium")
            .withSlaveInstanceType("m1.medium"));
    result = emr.runJobFlow(request);
    return result.getJobFlowId();
  }

  public String getStatusEmr(String clusterId){
    String state = null;
    List<ClusterSummary> clusters = emr.listClusters().getClusters();
    for (int i = 0; i < clusters.size(); i++) {
      if (clusters.get(i).getId().equals(clusterId)) {
        ClusterStatus status = clusters.get(i).getStatus();
        state = status.getState();
      }
    }
    if (state.contains("TERMINATED")) {
      return "terminated";
    }
    logger.info(getDnsMaster(clusterId));
    clusterSettings.get(clusterId).setUrl(getDnsMaster(clusterId));
    try {
      saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return state.toLowerCase();
  }
  
  public String getDnsMaster(String clusterId){
    String master = null;
    DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
    List<Reservation> reservations = describeInstancesRequest.getReservations();
    int count = 0;
    for (Reservation reservation : reservations) {
      List<com.amazonaws.services.ec2.model.Tag> tags = reservation
          .getInstances().get(0).getTags();
      for (int i = 0; i < tags.size(); i++) {
        if ((tags.get(i).getValue().equals(clusterId)) 
            || (tags.get(i).getValue().equals("MASTER"))){
          count++;
        }
      }
      if (count == 2)
        master = reservation.getInstances().get(0).getPublicDnsName();
      count = 0;
    }
    return master;
  }
  
  public List<ClusterSetting> getStatus() {
    String status = null;
    for (ClusterSetting cluster: clusterSettings.values()) {
      if (cluster.getType().equals("hadoop")) {
        status = getStatusEmr(cluster.getId());
        cluster.setStatus(status);
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
  
  public boolean remove(String clusterId, boolean snapshot)
      throws IOException {
    if (clusterSettings.get(clusterId).getType().equals("hadoop") || 
        clusterSettings.get(clusterId).getType().equals("spark")) {
      return removeEmrCluster(clusterId);
    }
    return false;
  }
  
  public boolean removeEmrCluster(String clusterId) {
    String jobFlowId = null;
    String name = clusterSettings.get(clusterId).getName();
    List<ClusterSummary> clusters = emr.listClusters().getClusters();
    for (int i = 0; i < clusters.size(); i++) {
      if (clusters.get(i).getName().equals(name)) {
        jobFlowId = clusters.get(i).getId();
        emr.terminateJobFlows(
            new TerminateJobFlowsRequest(Arrays.asList(new String[] {jobFlowId})));
        clusterSettings.remove(clusterId);
        try {
          saveToFile();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return true;
      }
    }
    return false;
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
