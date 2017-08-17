(ns yarn-runner.am)

(do
  (import '[org.apache.hadoop.con Configuration])
  (import '[org.apache.hadoop.yarn.conf YarnConfiguration])
  (import '[org.apache.hadoop.yarn.client.api.impl AMRMClientImpl NMClientImpl])
  (import '[org.apache.hadoop.yarn.api.records FinalApplicationStatus]))

(defn run []
  (println "hello world"))

(defn -main [& args]
  (def rm-con (doto (new ARMRClientImpl)
                    (.init (new YarnConfiguration))
                    .start))
  (.registerApplicationMaster rm-con "" 0 "")
  (run)
  (.unregisterApplicationMaster rm-con FinalApplicationStatus/SUCCEEDED "" "")
  )
  
  
