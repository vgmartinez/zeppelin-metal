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
import org.apache.zeppelin.clusters.Clusters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Application;
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
import com.amazonaws.services.elasticmapreduce.util.StepFactory;


/**
 * Interpreter Rest API
 *
 */
public class EmrClusterFactory implements Clusters {
  static Logger logger = LoggerFactory.getLogger(EmrClusterFactory.class);
  
  public static String clusterIdentifier = "";
  
  AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(
      new DefaultAWSCredentialsProviderChain());
  
  static AmazonEC2 ec2 = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
  ClusterImpl clusterImpl = new ClusterImpl();
  
  public EmrClusterFactory() {}

  @Override
  public void createCluster(String name, int nodes, String instance, Map<String, Boolean> apps) {

    String id = createClusterHadoop(name, nodes, instance, apps);
    ClusterSetting clustSetting = new ClusterSetting(id, name, nodes, 
        "starting", null, "", "hadoop", apps);
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
    List<StepConfig> appsToIntall = new LinkedList<StepConfig>();
    StepFactory stepFactory = new StepFactory();
    
    StepConfig enabledebugging = new StepConfig()
        .withName("Enable debugging")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
    
    appsToIntall.add(enabledebugging);
    
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
    
    BootstrapActionConfig boots = new BootstrapActionConfig("Install Hue",
            new ScriptBootstrapActionConfig("s3://elasticmapreduce/libs/hue/install-hue", null));
    
    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName(name)
      .withServiceRole("EMR_DefaultRole")
      .withJobFlowRole("EMR_EC2_DefaultRole")
      .withInstances(new JobFlowInstancesConfig()
        .withInstanceCount(nodes)
        .withKeepJobFlowAliveWhenNoSteps(true)
        .withMasterInstanceType(instance)
        .withSlaveInstanceType(instance));
    
    if (apps.get("hive")) {
      appsToIntall.add(installHive);
    } 
    if (apps.get("hue")) {
      appsToIntall.add(installHue);
      request
        .withBootstrapActions(boots);
    } 
    if (apps.get("spark")) {
      Application sparkApp = new Application()
        .withName("Spark");
    
      List<Application> spark = new ArrayList<Application>();
      spark.add(sparkApp);
      request
        .withReleaseLabel("emr-4.1.0")
        .withApplications(spark);
    } else {
      request
        .withAmiVersion("3.8.0");
    }
    
    request
      .withSteps(appsToIntall);
    
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
          urls.put("hue", dns + ":8888");          
        }
        
        urls.put("master", dns + ":9026");
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
  @Override
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
