(ns storm-trident-kafka-redis.core
    (:require [clojure.string :as string]
            [clj-time.core :refer [to-time-zone time-zone-for-offset]]
            [clj-time.local :refer [format-local-time]]
            [clj-time.coerce :as tc]
            [taoensso.timbre :refer [info debug warn]]
            [marceline.storm.trident :as t]
            [org.apache.storm.config :as config :refer [TOPOLOGY-DEBUG]]
            [com.rpl.specter :refer [select ALL]])
  (:import [org.apache.storm LocalCluster LocalDRPC StormSubmitter spout.SchemeAsMultiScheme]
           [org.apache.storm.trident TridentTopology]
           [org.apache.storm.trident.state StateType]
           [org.apache.storm.trident.operation.builtin MapGet]
           [org.apache.storm.trident.testing FixedBatchSpout MemoryMapState$Factory]
           [org.apache.storm.kafka ZkHosts StringScheme]
           [org.apache.storm.kafka.trident OpaqueTridentKafkaSpout TridentKafkaConfig]
           [org.apache.storm.redis.trident.state RedisMapState RedisMapState$Factory Options KeyFactory]
           [org.apache.storm.redis.common.config JedisPoolConfig JedisPoolConfig$Builder]
           [org.apache.storm.redis.common.mapper RedisStoreMapper RedisDataTypeDescription RedisDataTypeDescription$RedisDataType ])
  (:gen-class))

(t/deftridentfn parse-userinfo
  [tuple collector]
  (info "parse-userinfo" (t/first tuple))
  (when-let [message (t/first tuple)]
    (let [{readingTime "readingTime" userId "userId" logTime "logTime"} (as-> message $ (string/split $ #"\|") (nth $ 2)
                                                                              (string/split $ #"&") (map #(string/split % #"=" 2) $)
                                                                              (select [ALL #(= 2 (count %))] $)
                                                                              (into {} $))
          dt (-> readingTime Long/parseLong tc/from-long (to-time-zone (time-zone-for-offset +8)) (format-local-time :date) )]
      (info "parse-userinfo-result:" [dt userId logTime])
      (t/emit-fn collector dt userId logTime))))

(t/defcombineraggregator sum-fields
  ([] 0)
  ([tuple] (-> tuple (.getValueByField "logTime") Long/parseLong))
  ([t1 t2] (debug "sum-fields-t1-t2" [t1 t2]) (+ t1 t2)))

(defn build-topology []
  (let [trident-topology (TridentTopology.)
        kafka-config (doto (TridentKafkaConfig. (ZkHosts. "10.205.3.23,10.205.3.24,10.205.3.25")
                                                "log-BookBase.TLogExplanationServer_qd_readtime_yearmonthdayhour.log")
                       (-> .scheme (set! (SchemeAsMultiScheme. (StringScheme.))))
                       (-> .startOffsetTime (set! (kafka.api.OffsetRequest/LatestTime) ) ))
        kafka-trident-spout (OpaqueTridentKafkaSpout. kafka-config)
        redis-config (-> (JedisPoolConfig$Builder.) 
                         (.setHost "redis.storm.TXWX.db")
                         (.setPassword "....")
                         (.setPort 30000)
                         .build)
        redis-options (doto (Options.)
                        (-> .keyFactory (set! (reify KeyFactory (build [_ keys] (->> keys drop-last (list* "qd-readtime") (string/join ".")) )))))
        redis-factory (RedisMapState$Factory. redis-config StateType/OPAQUE redis-options)]
    (-> (t/new-stream trident-topology "qd-readtime" kafka-trident-spout)
        (t/parallelism-hint 1)
        (t/each ["str"] parse-userinfo ["dt" "userId" "logTime"])
        (t/project ["dt" "userId" "logTime"])
        (t/group-by ["dt" "userId" "logTime"])
        (t/persistent-aggregate redis-factory ["dt" "userId" "logTime"] sum-fields ["count"]))
    trident-topology))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "qd-readtime-local" {TOPOLOGY-DEBUG false} (.build (build-topology)))
    (Thread/sleep 10000)
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology name {} (.build (build-topology))))

(defn -main
  ([] (run-local!))
  ([name] (submit-topology! name)))

(comment
  (-main)
  (require '[taoensso.carmine :as car :refer [wcar]])

  (wcar {:spec {:host "redis.storm.TXWX.db" :port 30000 :password "...."}}
        (car/ping)
        #_(car/set "yuewen" "yuewen here!")
        #_(car/get "yuewen")
              (car/keys "*")) 
  )
