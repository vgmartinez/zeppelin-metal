# Metal for Apache Zeppelin

Zeppelin-metal is a module for Apache Zeppelin infrastructure management, for the moment it is only possible to EMR
and Redshift cluster. In EMR some applications like Hive, Hue and Spark are available.

For example you can set the spark cluster to Spark interpreter in Zeppelin the same for hive interpreter.

##AWS
Environment variable **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** is necessary  for use the cluster
or credentials file e.g

```
[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
##Installation
###Installation zeppelin-metal-server and zeppelin-metal-aws
For install zeppelin-metal the next steps is necessary:
* Clone the project into Apache Zeppelin home directory.
```
    git clone https://github.com/vgmartinez/zeppelin-metal.git
```
* Edit the pom file in Apache Zeppelin.
  In the pom file add new modules
```
    <module>./zeppelin-metal/zeppelin-metal-server</module>
    <module>./zeppelin-metal/zeppelin-metal-aws</module>
```
* Build the Apache Zeppelin project
* Add new module in the class path.
  In the file ```zeppelin-daemon.sh``` add the new modules
```
  # construct classpath
  if [[ -d "${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes" ]]; then
    ZEPPELIN_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes"
  fi
  
  if [[ -d "${ZEPPELIN_HOME}/zeppelin-zengine/target/classes" ]]; then
    ZEPPELIN_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-zengine/target/classes"
  fi
  
  if [[ -d "${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-aws/target/classes" ]]; then
    ZEPPELIN_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-aws/target/classes"
  fi
  
  if [[ -d "${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-server/target/classes" ]]; then
    ZEPPELIN_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-server/target/classes"
  fi
  
  addJarInDir "${ZEPPELIN_HOME}"
  addJarInDir "${ZEPPELIN_HOME}/lib"
  addJarInDir "${ZEPPELIN_HOME}/zeppelin-interpreter/target/lib"
  addJarInDir "${ZEPPELIN_HOME}/zeppelin-zengine/target/lib"
  addJarInDir "${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-server/target/lib"
  addJarInDir "${ZEPPELIN_HOME}/zeppelin-metal/zeppelin-metal-aws/target/lib"
  addJarInDir "${ZEPPELIN_HOME}/zeppelin-web/target/lib"
```
###Installation zeppelin-metal-web
For installation of ```zeppelin-metal-web``` only need pull from git repo.
```
  git remote add metal https://github.com/vgmartinez/zeppelin-web-metal.git
  git pull metal master
```
