(ns kafka-transformers.core)

(do
  (require '[cheshire.core :refer [parse-string generate-string]])
  (require '[clojure.data.codec.base64 :as b64])
  (require '[clojure.string :as s])
  (require '[clojure.java.io :as io])
  (import '[org.apache.kafka.clients.consumer KafkaConsumer])
  (import '[org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (import '[java.util.zip GZIPInputStream])
  )

(defn mk-prop [m] (reduce #(doto %1 (.put ((comp name key) %2) (val %2))) (new java.util.Properties) m)  )
(defn parse-records [j-records] (map #(-> % .value (parse-string true) ) j-records))

(def consumer-edn
  {:bootstrap.servers "10.132.37.39:9092"
   :group.id "larluo"
   :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"}
  )
(def producer-edn
  {:bootstrap.servers "10.132.37.39:9092"
   :key.serializer "org.apache.kafka.common.serialization.StringSerializer"
   :value.serializer "org.apache.kafka.common.serialization.StringSerializer"}
  )

(defn xdecode [s] (-> s .getBytes b64/decode clojure.java.io/input-stream GZIPInputStream. slurp))
(defn pretty-single [log-type decrypt]
  (let [m [:logLevel :logDate :logTS :sessionID :perfix :dbThreadNO :tranLevel :serviceInd :procName :message]
        showm [:logDate :logTS :logLevel :prefix :serviceInd :procName :message]
        logm ["[TRACE]" "[DEBUG]" "[INFO]" "[WARN]" "[ERROR]" "[FATAL]"]]
    (when (not= [""] decrypt)
      (-> decrypt (s/split #"#,#")
          (->> (map (juxt (comp m #(Integer/parseInt %) str first) #(subs % 1))) (into {})) 
          (update :logLevel (comp logm dec #(Integer/parseInt (or % "2")) ))
          (update :logDate #(or % "          ")) ;10
          (update :logTS #(or % "            ")) ;12
          (keep showm)  (->> (s/join " ")) )) )
  )
(defn pretty-encrypt [{log-type "log_type" :as m}]
  (update m :encrypt 
    #(-> % xdecode (s/split #"#;#") (->> (map (partial pretty-single log-type)) (s/join "\n")) ) )  )

(defn transform-message [message]
  (let [fields ["thread_no" "log_type" :encrypt]
        logm ["柜面输入接口" "网关输入接口" "核心输入接口" "输入接口" 
              "数据库日志" "输出接口" "核心输出接口" "网关输出接口"
              "柜面输出接口"]]
    (-> (zipmap fields (s/split message #"#;#")) 
        pretty-encrypt
        (doto println)
        (update "log_type" (comp logm dec #(Integer/parseInt %))) 
        (map fields)
        (->> (apply format "thread_no=%s log_type=%s\n%s")) ) 
    )
  )

#_(defn -main []
  (let [fields ["thread_no" "log_type" :encrypt]
        kafka-consumer (new KafkaConsumer (mk-prop consumer-edn))
        kafka-producer (new KafkaProducer (mk-prop producer-edn))]
    (.subscribe kafka-consumer ["logi_cores_pts-COMPRESS"])
    (transduce (comp
                (mapcat parse-records)
                (map #(update-in % [:message] transform-message))
                (map generate-string)
                #_(map #(doto % println))
                (map #(.send kafka-producer (new ProducerRecord "logi_cores_pts" % )))
                (filter (constantly false)))
               conj (repeatedly #(.poll kafka-consumer Long/MAX_VALUE))) )
  )
  
(defn -main []
  (->> "/home/larluo/work/git/my-repo/logtools/COMPRESS.tmp.20180712" io/reader line-seq 
       (take 3)
       (map transform-message)
       (mapv println)))  
(comment
  (def kafka-consumer (new KafkaConsumer (mk-prop consumer-edn)))
  (.subscribe kafka-consumer ["logi_cores_pts-COMPRESS"])
  (def data (.poll kafka-consumer Long/MAX_VALUE) )
  (def record (-> data parse-records first (update-in  [:message] transform-message) ) ) 

  (def kafka-producer (new KafkaProducer (mk-prop producer-edn)))
  (.send kafka-producer (new ProducerRecord "logi_cores_pts" record))

  
  )
 
