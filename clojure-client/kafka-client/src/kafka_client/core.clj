(ns kafka-client.core
  (:require [kinsky.client      :as client]
            [kinsky.async       :as async]
            [clojure.core.async :refer [go <! >!]]))

(import '[kafka.admin AdminUtils])

(def zk-admin (.apply kafka.utils.ZkUtils$/MODULE$ "127.0.0.1:2181" 10000 10000 false) ) 

(AdminUtils/createTopic zk-admin "aaa" 1 1 (new java.util.Properties) kafka.admin.RackAwareMode$Enforced$/MODULE$)
(AdminUtils/deleteTopic zk-admin "aaa")
(AdminUtils/fetchAllTopicConfigs zk-admin)

(let [p (client/producer {:bootstrap.servers "localhost:9092"}
                         (client/keyword-serializer)
                         (client/edn-serializer))]
  (client/send! p "aaa" :mygod {:ok :ok}))

(def kafka-con-in (client/consumer {:bootstrap.servers "localhost:9092"
                          :group.id          "mygroup"}
                         (client/keyword-deserializer)
                         (client/edn-deserializer)))
(client/subscribe! kafka-con-in "aaa")
(client/poll! kafka-con-in 1000)


