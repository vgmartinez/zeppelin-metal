package org.apache.zeppelin.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
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
public class ClusterFactory {
  static Logger logger = LoggerFactory.getLogger(ClusterFactory.class);
  
  private Map<String, ClusterSetting> clusterSettings =
      new HashMap<String, ClusterSetting>();
  String[] clusterClassList;
  String sts = null;
  String id = null;
  //static AWSCredentials credentials = (AWSCredentials) 
    //new ProfileCredentialsProvider("datalab-dev-admin")
      //.getCredentials();
  static AWSCredentials credentials = new BasicAWSCredentials("AKIAIJMBYRX2YAHZEA2Q",
      "eqqrkYQ7vUQWNSa6GimxHq6D70LzuKlc2dIbCakn");
  AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
  private RunJobFlowResult result;
  
  static AmazonEC2 ec2 = new AmazonEC2Client(credentials);
  
  public static AmazonRedshiftClient client = new AmazonRedshiftClient(credentials);
  public static String clusterIdentifier = "";
  
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();
  
  public ClusterFactory() {
    try {
      loadFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void addSpark(String name, int slaves)
      throws IOException {

    createSparkStandalone(name, slaves);
    ClusterSetting clustSetting = new ClusterSetting(name, slaves, "starting", "", "", "spark");
    clusterSettings.put(clustSetting.getId(), clustSetting);
    saveToFile();
  }
  
  public boolean createSparkStandalone(String name, int slaves) {
    try {
      ProcessBuilder builder = new ProcessBuilder("python", 
          "ec2/spark_ec2.py", "-k", "dataswarm",
          "-i", "dataswarm.pem", "-s", slaves + "", "launch", name);
      builder.redirectOutput(new File("logs/zeppelin-cluster-" + name + ".log"));
      builder.redirectError(new File("logs/zeppelin-cluster-" + name + ".out"));
      Process p = builder.start(); // throws IOException
      return true;
    }
    catch (IOException e) {
      logger.info("exception happened - here's what I know: " + e.getMessage());
      e.printStackTrace();
      return false;
    }
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
        .withLogUri("s3://dataswarm/logs/")
        .withInstances(new JobFlowInstancesConfig()
            .withEc2KeyName("dataswarm")
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
            .withEc2KeyName("dataswarm")
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withMasterInstanceType("m1.medium")
            .withSlaveInstanceType("m1.medium"));
    result = emr.runJobFlow(request);
    return result.getJobFlowId();
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
      else if (cluster.getType().equals("spark")) {
        String s, l = null;
        String name = cluster.getName();
        List<String> lines = new LinkedList<String>(); 
        try {
          Process p = Runtime.getRuntime().exec("ec2/spark_ec2.py get-master " + name);
          BufferedReader stdInput = new BufferedReader(new
              InputStreamReader(p.getInputStream()));
          while ((s = stdInput.readLine()) != null) {
            if (s.contains("ec2")) {
              status = s;
            }
          }
          stdInput.close();
          FileReader log = new FileReader(new File("logs/zeppelin-cluster-" + name + ".log"));
          BufferedReader readLog = new BufferedReader(log);
          while ((l = readLog.readLine()) != null) {
            lines.add(l);
          }
          readLog.close();
          if ((lines.size() > 1) && (lines.get(lines.size() - 1).equals("Done!"))) {
            cluster.setStatus("success");
            cluster.setUrl(status);
            saveToFile();
          } else {
            cluster.setStatus("starting");
            status = "starting";
          }
        }
        catch (IOException e) {
          logger.info("exception happened - here's what I know: ");
          e.printStackTrace();
          status = "error";
        }
      } else {
        try {
          status = getStatusRedshift(cluster.getId());
          cluster.setStatus(status);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
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
  
  /**
   * @param id of cluster
   * @throws IOException
   */
  public boolean remove(String clusterId, boolean snapshot)
      throws IOException {
    if (clusterSettings.get(clusterId).getType().equals("hadoop") || 
        clusterSettings.get(clusterId).getType().equals("spark")) {
      return removeEmrCluster(clusterId);
    } else {
      return removeRedshiftCluster(clusterId, snapshot);
    }
  }
  /**
   * 
   */
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
  
  /**
   * 
   */
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
  
  /**
   * @param id of cluster
   * @throws IOException
   */
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
  
  /**
   * Add new interpreter setting
   * @param name
   * @param slaves
   * @return
   */
  public void resizeMemoryCluster(String clusterid, int slaves){
    id = clusterid;
    clusterSettings.get(clusterid).setStatus("starting");
    String name = clusterSettings.get(clusterid).getName();
    int slv = clusterSettings.get(clusterid).getSlaves();
    int cant = (slaves / 7) - slv;
    clusterSettings.get(clusterid).setSlaves(cant + slv);
    try {
      saveToFile();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    try {
      ProcessBuilder builder = new ProcessBuilder("python", "ec2/spark_ec2.py", "-k", "dataswarm",
          "-i", "dataswarm.pem", "-s", cant + "", "resize", name);
      builder.redirectOutput(new File("logs/zeppelin-cluster-" + name + ".log"));
      builder.redirectError(new File("logs/zeppelin-cluster-" + name + ".out"));
      Process p = builder.start(); // throws IOException
    }
    catch (IOException e) {
      logger.info("exception happened - here's what I know: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
