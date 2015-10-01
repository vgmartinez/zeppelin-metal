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
 * Interpreter Rest API
 *
 */
public abstract class Clusters {
  
  public abstract void createCluster(String name, int nodes, String type);
  
  public abstract String getStatus(String clusterId);
}
