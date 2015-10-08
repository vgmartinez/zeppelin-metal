package org.apache.zeppelin.cluster.emr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.apache.zeppelin.clusters.ClusterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  ClusterImpl clusterImpl = new ClusterImpl();
  
  public EmrClusterFactory() {}

  public void createCluster(String name, int nodes, String instance, Map<String, Boolean> apps) {

    String id = createClusterHadoop(name, nodes, instance, apps);
    ClusterSetting clustSetting = new ClusterSetting(id, name, nodes, 
        "starting", null, "", "emr", apps);
    clustSetting.setApps(apps);
    clusterImpl.add(clustSetting);
    
    try {
      clusterImpl.saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public String createClusterHadoop(String name, int nodes, String instance, 
      Map<String, Boolean> apps){
    RunJobFlowResult result;
    StepFactory stepFactory = new StepFactory();
    
    StepConfig enabledebugging = new StepConfig()
        .withName("Enable debugging")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
    
    List<Application> applications = new ArrayList<Application>();
    
    if (apps.get("hive")) {
      Application hive = new Application()
        .withName("Hive");
      applications.add(hive);
    } 
    if (apps.get("hue")) {
      Application hue = new Application()
        .withName("Hue");
      applications.add(hue);
    } 
    if (apps.get("spark")) {
      Application spark = new Application()
        .withName("Spark");
      applications.add(spark);
    }
    
    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName(name)
      .withReleaseLabel("emr-4.1.0")
      .withSteps(enabledebugging)
      .withApplications(applications)
      .withServiceRole("EMR_DefaultRole")
      .withJobFlowRole("EMR_EC2_DefaultRole")
      .withInstances(new JobFlowInstancesConfig()
        .withInstanceCount(nodes)
        .withKeepJobFlowAliveWhenNoSteps(true)
        .withMasterInstanceType(instance)
        .withSlaveInstanceType(instance));
   
    result = emr.runJobFlow(request);
    return result.getJobFlowId();
  }

  public String getStatusEmr(String clusterId){
    String state = null;
    Map<String, String> urls = new HashMap<String, String>();
    
    List<ClusterSummary> EmrClusters = emr.listClusters().getClusters();
    for (ClusterSummary clusterSummary: EmrClusters) {
      if (clusterSummary.getId().equals(clusterId)) {
        ClusterStatus status = clusterSummary.getStatus();
        state = status.getState();
      }
    }
    if (state.contains("TERMINATED")) {
      clusterImpl.remove(clusterId);
      return "terminated";
    } else if (state.contains("WAITING") || state.contains("RUNNING")) {
      String dns = getDnsMaster(clusterId);
      if (!dns.isEmpty()) {
        ClusterSetting cl = clusterImpl.get(clusterId);
        if (cl.getApps().get("hue")) {
          urls.put("hue", "http://" + dns + ":8888");          
        }
        urls.put("master", "http://" + dns + ":8088");
        urls.put("dns", dns);
        cl.setUrl(urls);
      } 
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
  
  public String getStatus(String clusterId) {
    ClusterSetting cluster = clusterImpl.get(clusterId);
    String status = getStatusEmr(clusterId);
    cluster.setStatus(status);

    try {
      clusterImpl.saveToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return status;
  }
  
  public List<ClusterSetting> get() {
    return clusterImpl.list();
  }
  
  public void remove(String clusterId) {
    removeEmrCluster(clusterId);
  }
  
  public void removeEmrCluster(String clusterId) {
    emr.terminateJobFlows(
        new TerminateJobFlowsRequest(Arrays.asList(new String[] {clusterId})));
  }
  
  public void setClusterToInterpreter(String intId, String clustId) {
    List<ClusterSetting> clusterSettings = clusterImpl.list(); 
    List<ClusterSetting> settings = new LinkedList<ClusterSetting>(
        clusterSettings);
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
