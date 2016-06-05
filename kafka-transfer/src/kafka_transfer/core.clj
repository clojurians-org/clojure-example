(ns kafka-transfer.core)

(require '[clj-kafka.admin :as admin])
(require '[clj-kafka.zk :refer [brokers]])
(require '[clj-kafka.new.producer :refer [producer byte-array-serializer record send]])

(brokers {"zookeeper.connect" "127.0.0.1:2181"})
(with-open [zk (admin/zk-client "127.0.0.1:2181")]
  (if-not (admin/topic-exists? zk "my-topic")
    (admin/create-topic zk "my-topic"
                        {:partitions 3
                         :replication-factor 1
                         :config {"cleanup.policy" "compact"}})))
(with-open [p (producer {"bootstrap.servers" "127.0.0.1:9092"} (byte-array-serializer) (byte-array-serializer))]
  @(send p (record "my-topic" (.getBytes "hello world!"))))

(with-open [p (producer {"bootstrap.servers" "127.0.0.1:9092"} (byte-array-serializer) (byte-array-serializer))]
  (doseq [num (range 10000)] 
    @(send p (record "my-topic" (.getBytes (str num))))))

