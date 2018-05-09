(ns kafka2redis.core)

(do
  (require '[cheshire.core :refer [parse-string]])
  (import '[org.apache.kafka.clients.consumer KafkaConsumer])
  (import '[redis.clients.jedis JedisCluster HostAndPort])
  )

(defn mk-consumer [props]
  (let [j-props (new java.util.Properties)]
    ((comp dorun map) (fn [[k v]] (.put j-props (name k) v)) props)
    (new KafkaConsumer j-props)
    )
  )

(defn parse-records [j-records]
  (map #(-> % .value (parse-string true) :payload) j-records)
  )

(defn connect-redis [redis-con record]
  (println "count:" (.incr redis-con "larluo-test")  ) 
  )

(defn -main []
  (def kafka-consumer (mk-consumer
               {:bootstrap.servers  "localhost:9092"
                :group.id "larluo"
                :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
                :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"}))
  (def redis-con (new JedisCluster #{(new HostAndPort "localhost" (int 6379))})) 
  (.subscribe kafka-consumer ["connect-test"])
  (transduce (comp
            (mapcat parse-records)
            (map (partial connect-redis redis-con) )
            (filter (constantly false)))
    conj
    (repeatedly #(.poll kafka-consumer Long/MAX_VALUE)))
  )

(comment
  (-main)
  )
