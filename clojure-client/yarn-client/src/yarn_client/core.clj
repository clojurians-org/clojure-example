(ns yarn-client.core
  (:require [taoensso.timbre :refer [info debug warn set-level!]]
            [clojure.tools.trace :refer [trace trace-forms dotrace]])
  (:import [org.apache.hadoop.yarn.client.api YarnClient]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.util Records]
           [org.apache.hadoop.yarn.api.records ContainerLaunchContext])  )

(set-level! :trace)
(def conf (doto (new Configuration) (.set "yarn.resourcemanager.hostname" "192.168.1.3")) )
(def client (YarnClient/createYarnClient))
(do
    (.init client (new YarnConfiguration conf)) 
    (.start client)
    (.getYarnClusterMetrics client)
    (def app (-> client .createApplication))
    (def app-id (-> app .getNewApplicationResponse .getApplicationId))

    (def container-context (doto (Records/newRecord ContainerLaunchContext)
                             (.setCommands ["hostname"])) )
    (def app-context (.getApplicationSubmissionContext app))
    
    (doto app-context
      (.setAMContainerSpec container-context)) 
    
        
    (.stop client)
    )
