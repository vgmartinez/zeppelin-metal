package org.apache.zeppelin.cluster.utils;

import java.util.Map;

import org.apache.zeppelin.cluster.emr.ClusterSettingEmr;
import org.apache.zeppelin.cluster.rds.ClusterSettingRds;
import org.apache.zeppelin.cluster.redshift.ClusterSettingRedshift;

/**
*
*/
public class ClusterInfoSaving {
  public Map<String, ClusterSettingEmr> EMR;
  public Map<String, ClusterSettingRedshift> Redshift;
  public Map<String, ClusterSettingRds> RDS;
}
