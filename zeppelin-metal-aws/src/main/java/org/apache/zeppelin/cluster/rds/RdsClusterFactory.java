package org.apache.zeppelin.cluster.rds;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.rds.AmazonRDSAsyncClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.CreateDBInstanceRequest;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DeleteDBInstanceRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;

/**
 * Interpreter Rest API
 *
 */
public class RdsClusterFactory {
  static Logger logger = LoggerFactory.getLogger(RdsClusterFactory.class);

  AmazonRDSClient rds = new AmazonRDSClient(
      new DefaultAWSCredentialsProviderChain());
  AmazonRDSAsyncClient r = new AmazonRDSAsyncClient(new DefaultAWSCredentialsProviderChain());
  
  static AmazonEC2 ec2 = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
  
  public RdsClusterFactory() {}

  public ClusterSettingRds createCluster(String name, String instanceType, int storage,
      String user, String passw, String engine, String version) {
    
    ClusterSettingRds clustSetting = new ClusterSettingRds(name, user, passw,
        "starting", null, "", "rds", instanceType, engine, storage, version);
    
    createClusterRds(name, instanceType, storage, user, passw, engine, version);
    
    return clustSetting;
  }
  
  public void createClusterRds(String name, String instanceType, int storage, 
      String user, String passw, String engine, String version){
    
    CreateDBInstanceRequest postgres = new CreateDBInstanceRequest()
      .withEngine(engine)
      .withEngineVersion(version)
      .withAllocatedStorage(storage)
      .withDBInstanceIdentifier(name)
      .withDBName(name)
      .withMasterUsername(user)
      .withMasterUserPassword(passw)
      .withDBInstanceClass(instanceType);
    
    rds.createDBInstance(postgres);
  }

  public String getStatus(String clusterId){
    String state = null;
    String status = null;
    List<DBInstance> instances;
    DescribeDBInstancesResult db = rds.describeDBInstances(new DescribeDBInstancesRequest()
      .withDBInstanceIdentifier(clusterId));
    
    instances = db.getDBInstances();  
    for (DBInstance instance: instances) {
      state = instance.getDBInstanceStatus();
    }
    switch (state) {
        case "available":
          status = "running";
          break;
        case "creating":
          status = "starting";
          break;
        case "deleting":
          status = "deleting";
          break;
        case "backing-up":
          status = "starting";
          break;
    }
    return status;
  }
  
  public String getEndpoint(String clusterId){
    List<DBInstance> instances;
    String endpoint = null;
    DescribeDBInstancesResult db = rds.describeDBInstances(new DescribeDBInstancesRequest()
      .withDBInstanceIdentifier(clusterId));
  
    instances = db.getDBInstances();  
    for (DBInstance instance: instances) {
      endpoint = instance.getEndpoint().getAddress();
    }
    return endpoint;
  }
  
  public void remove(String clusterId) {
    rds.deleteDBInstance(new DeleteDBInstanceRequest()
        .withDBInstanceIdentifier(clusterId)
        .withSkipFinalSnapshot(true));
  }
}
