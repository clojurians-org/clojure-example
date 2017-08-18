(ns yarn-runner.driver)

(do
  (require '[manifold.defereed :refer [chain]])
  (require '[manifold.stream :as s])
  (require '[aleph.tcp :as tcp])
  (require '[gloss.core :refer [string finite-frame compile-frame]])
  (require '[gloss.io :refer [encode decode-stream]])

  (require '[clojure.tools.nrepl :as repl])
  (require '[clojure.tools.nrepl.server :rfer [start-server stop-server]])

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

(defn wrap-duplex-stream [proto s]
  (let [out (s/stream)]
    (s/connect (s/map (partial encode proto) out) $)
    (s/splice out (decode-stream s proto))))

(defn start-server [f proto pots]
  (let [server-handler (fn [f s info] (s/connect (s/map f s) s))]
    (tcp/start-server (fn [s info] (server-handler f (wrap-duplex-stream proto s) info))
                      opts)))
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
                        (.setCommands ["java clojure.main -m yarn-runner.am {:driver-host 172.30.115.59 :driver-port 1111} 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"])))

  (def app-context (doto (-> yarn-con .createApplication .getApplicationSubmissionContext)
                         (.setApplicationName "clojure-yarn-app")
                         (.setAMContainerSpec am-context)
                         (.setMaxAppAttempts 1)
                         (.setResource (doto (new ResourcePBImpl) (.setMemory 512) (.setVirtualCores 1)))))

  (def worker-endpoints (atom {}))
  (def server-handle
    (start-server
      (fn [{:keys [id {:keys [host port] :as data}]}]
        (swap! worker-endpoints assoc id {:data data :status :arrived})
        (with-open [con (repl/connect :host host :port port)]
          (-> (repl/client con 5000)
              (repl/message {:op :eval :code "(reduce + [1 2 3])"})
              doall
              println))
        (Thread/sleep 50000)
        "finished")
      (as-> (string :utf-8) $ (finite-frame :uinit32 $) (compile-frame $ pr-str clojure.edn/read-string))
      {:port 1111}))

  (.submitApplication yarn-con app-context)
  (-> (.getApplicationReport yarn-con (.getApplicationId app-context)) .getYarnApplicationState)
  (-> (.getApplicationReport yarn-con (.getApplicationId app-context)) .getDiagnostics)

  (.killApplication yarn-con app-id)
  (.killApplication yarn-con (ApplicationPBImpl/newInstance 1502103460022 515))
  )
