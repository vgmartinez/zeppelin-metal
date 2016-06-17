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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zeppelin.cluster.utils.ClusterInfoSaving;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Interpreter Rest API
 *
 */
public class EmrClusterFactory {
  static Logger logger = LoggerFactory.getLogger(EmrClusterFactory.class);
  public static String clusterIdentifier = "";

  AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(
      new DefaultAWSCredentialsProviderChain());
  static AmazonEC2 ec2 = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
  private Map<String, ClusterSettingEmr> EMRCluster =
      new HashMap<String, ClusterSettingEmr>();
  private Gson gson = new Gson();
  private ZeppelinConfiguration conf = new ZeppelinConfiguration();

  public EmrClusterFactory() {}

  public void create(ClusterSettingEmr cluster){
    RunJobFlowResult result;
    StepFactory stepFactory = new StepFactory();
    StepConfig enabledebugging = new StepConfig()
        .withName("Enable debugging")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

    List<Application> applications = new ArrayList<Application>();

    if (cluster.getApps().get("hive")) {
      Application hive = new Application()
          .withName("Hive");
      applications.add(hive);
    } 
    if (cluster.getApps().get("hue")) {
      Application hue = new Application()
          .withName("Hue");
      applications.add(hue);
    } 
    if (cluster.getApps().get("spark")) {
      Application spark = new Application()
          .withName("Spark");
      applications.add(spark);
    }
    RunJobFlowRequest request = new RunJobFlowRequest()
        .withName(cluster.getName())
        .withReleaseLabel("emr-4.1.0")
        .withSteps(enabledebugging)
        .withApplications(applications)
        .withServiceRole("EMR_DefaultRole")
        .withJobFlowRole("EMR_EC2_DefaultRole")
        .withInstances(new JobFlowInstancesConfig()
            .withInstanceCount(cluster.getSlaves())
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withMasterInstanceType(cluster.getInstanceType())
            .withSlaveInstanceType(cluster.getInstanceType()));

    result = emr.runJobFlow(request);
    String id = result.getJobFlowId();
    cluster.setId(id);
  }

  public String getStatus(String clusterId){
    String state = null;
    String status = null;

    List<ClusterSummary> EmrClusters = emr.listClusters().getClusters();
    for (ClusterSummary clusterSummary: EmrClusters) {
      if (clusterSummary.getId().equals(clusterId)) {
        ClusterStatus clStatus = clusterSummary.getStatus();
        state = clStatus.getState();
      }
    }
    switch (state) {
    case "WAITING":
      status = "running";
      break;
    case "RUNNING":
      status = "starting";
      break;
    case "STARTING":
      status = "starting";
      break;
    case "TERMINATED":
      status = "deleting";
      break;
    case "BOOTSTRAPING":
      status = "starting";
      break;
    default:
      status = "deleting";
      break;
    }
    return status;
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

  public void remove(String clusterId) {
    emr.terminateJobFlows(
        new TerminateJobFlowsRequest(Arrays.asList(new String[] {clusterId})));
  }

  public void setClusterToInterpreter(String intId, String clustId) {
    if (clustId.equals("")) {
      for (ClusterSettingEmr setting : EMRCluster.values()) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      } 
    } else {
      for (ClusterSettingEmr setting : EMRCluster.values()) {
        if (setting.getSelected().equals(intId)) {
          setting.setSelected("");
        }
      }
      EMRCluster.get(clustId).setSelected(intId);
    }
    saveToFile();
  }

  public void saveToFile() {
    String jsonString;

    ClusterInfoSaving info = new ClusterInfoSaving();
    info.EMR = EMRCluster;
    
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
  }

  public List<Object> getStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  public void remove() {
    // TODO Auto-generated method stub

  }
}
