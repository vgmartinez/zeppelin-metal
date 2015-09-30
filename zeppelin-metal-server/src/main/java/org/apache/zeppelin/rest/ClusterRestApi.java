package org.apache.zeppelin.rest;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.cluster.emr.EmrClusterFactory;
import org.apache.zeppelin.cluster.redshift.RedshiftClusterFactory;
import org.apache.zeppelin.cluster.utils.ClusterSetting;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestHadoop;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestRedshift;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestSpark;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Cluster Rest API
 *
 */
@Path("/cluster")
@Produces("application/json")
public class ClusterRestApi {
  Logger logger = LoggerFactory.getLogger(ClusterRestApi.class);
  
  EmrClusterFactory clusterFactory = new EmrClusterFactory();
  RedshiftClusterFactory redshiftFactory = new RedshiftClusterFactory();
  Gson gson = new Gson();

  public ClusterRestApi() {
    
  }
  
  /**
  * List all cluster
  * @return
  */
  @GET
  @Path("setting")
  public Response listSettings() {
    List<ClusterSetting> clusterSettings = null;
    clusterSettings = clusterFactory.get();
    logger.info("SETTING" + clusterSettings.toString());
    return new JsonResponse(Status.OK, "", clusterSettings).build();
  }
  /**
  * Add new cluster setting
  * @param name
  * @param slaves
  * @return
  * @throws IOException
  */
  @POST
  @Path("setting/{type}")
  public Response newSettings(@PathParam("type") String type, String message) throws IOException {
    if (type.equals("spark")) {
      NewClusterSettingRequestSpark request = gson.fromJson(message,
          NewClusterSettingRequestSpark.class);
      clusterFactory.addSpark(request.getName(), 
          request.getSlaves());
    } else if (type.equals("hadoop")) {
      NewClusterSettingRequestHadoop request = gson.fromJson(message,
          NewClusterSettingRequestHadoop.class);
      clusterFactory.addHadoop(request.getName(), 
          request.getSlaves());
    } else {
      NewClusterSettingRequestRedshift request = gson.fromJson(message,
          NewClusterSettingRequestRedshift.class);
      redshiftFactory.addRedshift(request.getName(), 
          request.getSlaves(), request.getType());
    }
    return new JsonResponse(Status.ACCEPTED, "").build();
  }
  
  /**
  * Cluster status check
  * @return
  */
  @GET
  @Path("status")
  public Response getStatusCluster(@PathParam("clusterId") String clusterId) {
    List<ClusterSetting> clusterSeeting = clusterFactory.getStatus();
    return new JsonResponse(Status.OK, clusterSeeting).build();
  }
  
  /**
  * Delete cluster
  * @return
  */
  @DELETE
  @Path("setting/{settingId}/{snapshot}")
  public Response removeSetting(@PathParam("settingId") String settingId,
      @PathParam("snapshot") String snapshot) throws IOException {
    logger.info("Remove interpreterSetting {}", settingId);
    clusterFactory.remove(settingId, Boolean.parseBoolean(snapshot));
    return new JsonResponse(Status.OK).build();
  }
  
  /**
   * set cluster to interpreter
   * @throws IOException
   */
  @PUT
  @Path("set/{intId}")
  public Response setInt(@PathParam("intId") String intId, 
      String clustId) throws IOException {
    logger.info("Interpreter id {}, cluster id {}", intId, clustId);
    clusterFactory.setClusterToInterpreter(intId, clustId);
    return new JsonResponse(Status.OK).build();
  }
  
  /**
   * resize cluster
   * @throws IOException
   */
  @PUT
  @Path("setting/{clusterId}")
  public Response resizeMemory(@PathParam("clusterId") String clusterId, 
      String request) throws IOException {
    logger.info("request: " + request);
    
    return new JsonResponse(Status.OK).build();
  }
}
