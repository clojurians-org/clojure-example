(ns storm-trident-drpc.core
  (:require [clojure.string :as string]
            [marceline.storm.trident :as t])
  (:import [org.apache.storm LocalCluster LocalDRPC StormSubmitter]
           [org.apache.storm.trident TridentTopology]
           [org.apache.storm.trident.operation.builtin MapGet]
           [org.apache.storm.trident.testing FixedBatchSpout MemoryMapState$Factory ])
  (:gen-class))

(def sentences ["lord ogdoad"
                "master of level eight shadow world"
                "the willing vessel offers forth its pure essence"])

(t/deftridentfn split-args
  [tuple coll]
  (when-let [args (t/first tuple)]
    (let [words (string/split args #" ")]
      (doseq [word words] (t/emit-fn coll word)) )))

(t/defcombineraggregator count-words
  ([] 0)
  ([tuple] 1)
  ([t1 t2] (+ t1 t2)))

(defn build-topology [drpc]
  (let [trident-topology (TridentTopology.)
        spout (doto (FixedBatchSpout. (t/fields "sentence") 3  (into-array (map t/values sentences)))
                (.setCycle true))
        word-counts (->  (t/new-stream trident-topology "word-counts" spout)
                        (t/each ["sentence"] split-args ["word"])
                        (t/group-by ["word"])
                        (t/persistent-aggregate (MemoryMapState$Factory.) count-words ["count"]))]
    (-> (t/drpc-stream trident-topology "words" drpc)
        (t/each ["args"] split-args ["word"])
        (t/project ["word"])
        (t/group-by ["word"])
        (t/state-query word-counts ["word"] (MapGet.) ["count"]))
    trident-topology))

(defn run-local! []
  (let [cluster (LocalCluster.)
        drpc (LocalDRPC.)]
    (.submitTopology cluster "word-count" {} (.build  (build-topology drpc)))
    (Thread/sleep 10000)
    (let [results (.execute drpc "words" "evil vessel ogdoad")]
      (.shutdown cluster)
      results)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology name {} (.build (build-topology nil))))

(defn -main
  ([] (run-local!))
  ([name] (submit-topology! name)))

(comment
  (import '[org.apache.storm Config utils.DRPCClient])
  (def conf (doto (Config.)
              (.setDebug false)
              (.put "storm.thrift.transport" "org.apache.storm.security.auth.SimpleTransportPlugin")
              (.put Config/STORM_NIMBUS_RETRY_TIMES 3)
              (.put Config/STORM_NIMBUS_RETRY_INTERVAL 10)
              (.put Config/STORM_NIMBUS_RETRY_INTERVAL_CEILING 20)
              (.put Config/DRPC_MAX_BUFFER_SIZE 1048576)))
  (defonce drpc (DRPCClient. conf "10.58.97.140" 3772))
  (.execute drpc "words" "evil vessel ogdoad")
  )
