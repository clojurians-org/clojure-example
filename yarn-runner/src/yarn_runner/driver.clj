(ns yarn-runner.driver)

(do
  (import '[org.apache.hadoop.conf Configuration])
  (import '[org.apache.hadoop.yarn.conf YarnConfiguration]
  (import '[org.apache.hadoop.yarn.client.api.impl YarnClientImpl])
  (import '[org.apache.hadoop.yarn.api.records
             LocalResourceType LocalResourceVisibility
             impl.pb.ApplicationIdPBImpl
             impl.pb.URLPBImpl impl.pb.LocalResourcePBImpl
             impl.pb.ContainerLaunchContextPBImpl
             impl.pb.ResourcePBImpl impl.pb.ApplicationSubmissionContextPBImpl])
  (import '[org.apache.hadoop.yarn.api ApplicationConstants ApplicationConstants$Environment])
  (import '[org.apache.hadoop.hdfs DFSClient])))


(comment
  (def hdfs-con (new DFSClient (new java.net.URI "hdfs://172.30.115.59") (new Configuration)))
  (def yarn-con (doto (new YarnClientImpl)
                      (.init (new YarnConfiguration
                               (doto (new Configuration) (.set "yarn.resourcemanager.address" "172.30.115.59"))))
                      .start))

  (def clojure-jar-path "/tmp/larluo/yarn-runner-0.1.0-SNAPSHOT-standalone.jar")
  (def clojure-jar (.getFileInfo hdfs-con clojure-jar-path))
  (def clojure-jar-resource
    (doto (new LocalResourcePBImpl)
          (.setResource (doto (new URLPBImpl) (setScheme "hdfs") (setHost "172.30.115.59" (.setFile clojure-jar-path))))
          (.setSize (.getLen clojure-jar))
          (.setTimestamp (.getModificationTime clojure-jar))
          (.setType LocalResourceType/FILE)
          (.setVisibility LocalResourceVisibility/PUBLIC)))
  (def am-context (doto (new ContainerContextPBImpl)
                        (.setLocalResources {"clojure.jar" clojure-jar-resource})
                        (.setEnvironment {"CLASSPATH" "$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:$PWD/*"})
                        (.setCommands ["java clojure.main -m yarn-runner.am 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"])))

  (def app-context (doto (-> yarn-con .createApplication .getApplicationSubmissionContext)
                         (.setApplicationName "clojure-yarn-app")
                         (.setAMContainerSpec am-context)
                         (.setMaxAppAttempts 1)
                         (.setResource (doto (new ResourcePBImpl) (.setMemory 512) (.setVirtualCores 1)))))

  (.submitApplication yarn-con app-context)
  (-> (.getApplicationReport yarn-con (.getApplicationId app-context)) .getYarnApplicationState)
  (-> (.getApplicationReport yarn-con (.getApplicationId app-context)) .getDiagnostics)

  (.killApplication yarn-con app-id)
  (.killApplication yarn-con (ApplicationPBImpl/newInstance 1502103460022 515))
  )
