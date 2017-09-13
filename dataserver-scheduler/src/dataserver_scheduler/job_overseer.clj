(ns dataserver-scheduler.job-overseer)

(do
  (require '[dataserver-scheduler.task-trigger :as trigger] :reload)
  (require '[dataserver-scheduler.task-dispatcher :as dispatcher] :reload)
  (require '[dataserver-scheduler.task-driver :as driver] :reload)

  (require '[clojure.core.async :refer [chan >!! <!! close! timeout alts!! poll!]])
  (require '[cheshire.core :refer [generate-string parse-string]])

  (import '[org.apache.curator.framework CuratorFrameworkFactory])
  (import '[org.apache.curator.retry ExponentialBackoffRetry])
  )

(defn ack-next [event-source [ack-chain [zk-con zk-root]] [event-type event-id event-data]]
  ((comp dorun map)
    (fn [event-target]
      (let [channel-zpath (format "%s/channels/%s/from_%s" zk-root (name event-target) (name event-source))
            _ (when-not (-> zk-con .checkExists (forPath channel-zpath))
                (-> zk-con .create .creatingParentsIfNeeded (.forPath channel-zpath)))
            message-id (some-> zk-con .getChildren (.forPath channel-zpath) sort last (subs 0 16) Integer/parseInt inc)
            event-zpath (format "%s/%016d_%s" channel-zpath (or message-id 1) event-id)]
        (-> zk-con .create .creatingParentsIfNeeded (.forPath event-zpath))
        (when event-data (-> zk-con .setData (.forPath event-zpath (.getBytes (generate-string event-data)))))
        ((get-in ack-chain [event-target :handler]) [event-source event-target] [ack-chain [zk-con zk-root]] [(or message-id 1) event-type event-id])))
    (get-in ack-chain [event-source :to event-type])))

(comment
  (def ack-chain
    {:trigger {:to {:dispatcher-task [:dispatcher]}
               :handler trigger/receive
     :dispatcher {:to {:driver-task [:driver]
                       :recovery-task [:trigger]}
                  :handler dispatcher/receive}
     :driver {:to {:finish-task [:dispatcher]}
              :handler driver/receive}
     :overseer {:to {:start-task-consumer [:trigger]
                     :stop-task-consumer [:trigger]
                     :notify-task-consumer [:trigger]}}}})
  (def zk-url "")
  (def zk-con (doto (CuratorFrameworkFactory/newClient zk-url (new ExponentialBackoffRetry 10000 60)) .start))
  (def zk-root "/solr5/scheduler")

  (ack-next :overseer [ack-chain [zk-con zk-root]] [:start-task-consumer nil])
  (ack-next :overseer [ack-chain [zk-con zk-root]] [:notify-task-consumer nil])
  (ack-next :overseer [ack-chain [zk-con zk-root]] [:stop-task-consumer nil])
  
   ((comp dec count re-find)  #"(11)" "111111" ) 
  ((comp count re-find) #"1+" "1110000") 
  ((comp first sequence)
    (comp (partition-by identity)
          (filter #(some #{\1} %))
          (map count)) 
    "1110000")


  (partition-by identity )
  )
