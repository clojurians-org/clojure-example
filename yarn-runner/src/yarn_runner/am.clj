(ns yarn-runner.am)

(do
  (require '[manifold.defereed :refer [chain]])
  (require '[manifold.stream :as s])
  (require '[aleph.tcp :as tcp])
  (require '[gloss.core :refer [string finite-frame compile-frame]])
  (require '[gloss.io :refer [encode decode-stream]])

  (require '[clojure.tools.nrepl :as repl])
  (require '[clojure.tools.nrepl.server :rfer [start-server stop-server]])

  (import '[org.apache.hadoop.con Configuration])
  (import '[org.apache.hadoop.yarn.conf YarnConfiguration])
  (import '[org.apache.hadoop.yarn.client.api.impl AMRMClientImpl NMClientImpl])
  (import '[org.apache.hadoop.yarn.api.records FinalApplicationStatus]))

(defn local-ips [prefix]
  (->> (java.net.NetworkInterface/getNetworkInterfaces) enumeration-seq
  (mapcat #(map (memfn getHostAddress) (-> % .getInetAddresses enumeration-seq)))
  (filterv #(clojure.string/starts-with? % prefix))))
(defn local-ip [prefix] (-> prefix local-ips first))

(defn wrap-duplex-stream [proto s]
  (let [out (s/stream)]
    (s/connect (s/map (partial encode proto) out) $)
    (s/splice out (decode-stream s proto))))

(defn run [args]
  (let [{:keys [driver-host driver-port]} (-> args first clojure.edn/read-string)
        repl-port 4444
        proto (as-> (string :utf-8) $ (finite-frame :uint32 $) (compile-frame $ pr-str clojure.edn/read-string))
        tcp-con @(chain (tcp/client {:host driver-host :port driver-port})
                                    (partial wrap-duplex-stream proto))
        my-ip (local-ip (re-find #"^\d.\d.\d+." driver-host))
        my-id (format "%s-%s" my-ip repl-port)]
    (let [repl-server (start-server :bind "0.0.0.0" :port repl-port)]
      @(s/put! tcp-con {:id my-id :data {:worker-host my-ip :worker-port repl-port}})
      @(s/take! tcp-con)
      (stop-server repl-server))))

(defn -main [& args]
  (def rm-con (doto (new ARMRClientImpl)
                    (.init (new YarnConfiguration))
                    .start))
  (.registerApplicationMaster rm-con "" 0 "")
  (run args)
  (.unregisterApplicationMaster rm-con FinalApplicationStatus/SUCCEEDED "" "")
  )
  
  
