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

import org.apache.zeppelin.cluster.ClusterFactory;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestHadoop;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestRds;
import org.apache.zeppelin.rest.message.NewClusterSettingRequestRedshift;
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
  
  ClusterFactory clusterFactory = new ClusterFactory();
  Gson gson = new Gson();

  public ClusterRestApi() {
    
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
    if (type.equals("emr")) {
      NewClusterSettingRequestHadoop request = gson.fromJson(message,
          NewClusterSettingRequestHadoop.class);
      clusterFactory.createCluster(request.getName(), 
          request.getInstance(), type, request.getSlaves(), request.getApp());
    } else if (type.equals("redshift")) {
      NewClusterSettingRequestRedshift request = gson.fromJson(message, 
          NewClusterSettingRequestRedshift.class);
      clusterFactory.createCluster(request.getName(),
          request.getInstance(), type, request.getSlaves(), null, 
          request.getUser(), request.getPassw());
    } else if (type.equals("rds")) {
      NewClusterSettingRequestRds request = gson.fromJson(message, 
          NewClusterSettingRequestRds.class);
      clusterFactory.createCluster(request.getName(),
          request.getInstance(), type, request.getStorage(), request.getUser(), 
          request.getPassw(), request.getEngine(), request.getVersion());
    }
    
    return new JsonResponse(Status.ACCEPTED, "").build();
  }
  
  /**
  * Get status of clusters
  * @return
  */
  @GET
  @Path("status")
  public Response getStatusCluster() {
    List<Object> clusterSettings = null;
    clusterSettings = clusterFactory.getStatus();
    return new JsonResponse(Status.OK, clusterSettings).build();
  }
  
  /**
  * Delete cluster
  * @return
  */
  @DELETE
  @Path("setting/{clusterType}/{clusterId}")
  public Response removeSetting(@PathParam("clusterType") String clusterType,
      @PathParam("clusterId") String clusterId)
      throws IOException {
    logger.info("Remove interpreterSetting {}", clusterId);
    clusterFactory.remove(clusterType, clusterId);
    return new JsonResponse(Status.OK).build();
  }
  
  /**
   * set cluster to interpreter
   * @throws IOException
   */
  @PUT
  @Path("set/{intId}/{clusterType}/{clusterId}")
  public Response setInt(@PathParam("intId") String intId, 
      @PathParam("clusterType") String clusterType, 
      @PathParam("clusterId") String clusterId) throws IOException {
    logger.info("Interpreter id {}, cluster id {}", intId, clusterId);
    clusterFactory.setClusterToInterpreter(intId, clusterType, clusterId);
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
