(ns dataserver-scheduler.task-dispatcher)

(do
  (require '[cheshire.core :refer [generate-string parse-string]])

  (import '[org.apache.curator.framework CuratorFrameworkFactory])
  (import '[org.apache.curator.retry ExponentialBackoffRetry])
  )

(defn init [zk-con]
  (def default-scheduler-env-conf {:max-running-tasks 10
                                   :max-accum-cost 100
                                   :max-single-cost 80})
  (-> zk-con .create .creatingParentsIfNeeded (.forPath "/solr5/scheduler/scheduler-env.default.json"))
  (-> zk-con .setData (.forPath "/solr5/scheduler/scheduler-env.default.json" (.getBytes (generate-string default-scheduler-env-conf))))
  (-> zk-con .create .creatingParentsIfNeeded (.forPath "/solr5/scheduler/scheduler-env.last-time.json"))
  (-> zk-con .setData (.forPath "/solr5/scheduler/scheduler-env.last-time.json" (.getBytes "{}")))
  (-> zk-con .create .creatingParentsIfNeeded (.forPath "/solr5/scheduler/scheduler-env.current.json"))
  (-> zk-con .setData (.forPath "/solr5/scheduler/scheduler-env.current.json" (.getBytes "{}")))
  (-> zk-con .create .creatingParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/state.json"))
  (-> zk-con .setData (.forPath "/solr5/scheduler/002_dispatcher/state.json" (.getBytes "{}")))
  (-> zk-con .create .creatingParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/metrics.json"))
  (-> zk-con .setData (.forPath "/solr5/scheduler/002_dispatcher/metrics.json" (.getBytes "{}")))
  )

(defn parse-recovering-task-id [event-id]
  (let [[f1 f2 f3] (clojure.string/split event-id #"_" 3)]
    (when (re-find #"^R\d{3}$" f2) f3)))

(defn refresh-scheduler-env [[zk-con zk-root]]
  (let [default-env (-> zk-con .getData (.forPath (format "%s/scheduler-env.default.json zk-root")) String. (parse-string true))
        current-env (-> zk-con .getData (.forPath (format "%s/scheduler-env.current.json zk-root")) String. (parse-string true))]
    (merge default-env current-env)))

(defn refresh-dispatcher-state [[zk-con zk-root]]
  (-> zk-con .getData (.forPath (format "%s/002_dispatcher/state.json" zk-root)) String. (parse-string true)))

(defn refresh-job-spec [task-id [zk-con zk-root]]
  (-> zk-con .getData (forPath (format "%s/configs/%s/specs.json" zk-root (subs task-id 30))) String. (parse-string true)))

(defn has-running-job? [task-id [zk-con zk-root]]
  (some #{task-id} (-> zk-con .getChildren (.forPath (format "%s/002_dispatcher/001_running" zk-root)) (->> map #(subs % 30)))))

(defn has-isolating-job? [task-id [zk-con zk-root]]
  (some #{task-id} (-> zk-con .getChildren (.forPath (format "%s/002_dispatcher/002_isolating" zk-root)) (->> map #(subs % 30)))))

(defn has-recovering-job? [task-id [zk-con zk-root]]
  (some #{task-id} (-> zk-con .getChildren (.forPath (format "%s/002_dispatcher/003_recovering" zk-root)) (->> map #(subs (parse-recovering-task-id %) 30)))))

(defn has-failed-job? [task-id [zk-con zk-root]]
  (some #{task-id} (-> zk-con .getChildren (.forPath (format "%s/002_dispatcher/004_failed" zk-root)) (->> map #(subs % 30)))))

(defn task-ready? [task-id [zk-con zk-root] {{single-cost :cost} :resource :as job-spec}]
  (let [{:keys [max-running-task max-accum-cost max-single-cost]} (refresh-scheduler-env [zk-con zk-root])
        {:keys [running-tasks-num accum-cost-val]} (refresh-dispatcher-state [zk-con zk-root])]
    (and (or (nil? max-running-tasks) (<= (inc (or running-tasks-num 0)) max-running-tasks))
         (or (nil? max-accum-cost (<= (+ (or single-cost 0) (or accum-aval 0)) max-accum-cost)))
         (or (nil? max-single-cost) (<= (or single-cost 0) max-single-cost))
         ((complement has-running-job?) task-id [zk-con zk-root])
         ((complement has-isolating-job?) task-id [zk-con zk-root]) 
         ((complement has-recovering-job?) task-id [zk-con zk-root]) 
         ((complement has-failed-job?) task-id [zk-con zk-root]) )))

(defn drop-in-blocking [task-id [zk-con zk-root] {}]
  (-> zk-con .create ))

(defmulti receive-event (fn [event-source [ack-chain [zk-con zk-root]] [message-id event-type event-id]] [event-source event-type]))
(defonce receive-lck (new Object))
(defn receive [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  (locking receive-lck
    (try (receive-event [event-soure event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id])
      (finally (-> zk-con .delete (.forPath (format "%s/channels/%s/from_%s/%016d_%s" zk-root (name event-target) (name event-source message-id event-id))))))))

(defmethod receive-event [:trigger :dispatcher-task]
  [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  (println [:dispatcher :receive-event :route [:trigger :dispatcher-task] :args [event-source [ack-chain [zk-con zk-root]] [message-id event-type event-id]]])
  (let [recovering-task-id (parse-recovering-task-id event-id)
        task-id (or recovering-task-id event-id)]
    (if (task-ready? task-id [zk-con zk-root] (refresh-job-spec task-id [zk-con zk-root]))
      (ack-next [:driver :from-dispatcher] task-id [zk-con zk-root])
      (drop-in-blocking task-id [zk-con zk-root]))
    (when recovering-task-id (-> zk-con .delete (.forPath (format "%s/003_recovering/%s" recovering-task-id)))) ))

(defmethod receive-event [:driver :finish-task]
  [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  )

(comment
  (def zk-url "")
  (def zk-con (doto (CuratorFrameworkFactory/newClient zk-url (new ExponentialBackoffRetry 10000 60)) .start))
  (def zk-root "/solr5/scheduler")

  (receive :from-trigger [zk-con zk-root] ["0000000000000001" "2017-09-06T18:00:00.000+08:00_larluo"])
  (receive :from-trigger [zk-con zk-root] ["0000000000000001" "2017-09-06T18:00:00.000+08:00_R001_2017-09-06T18:00:00.000+08:00_larluo"])
  (parse-recovering-task-id "2017-09-06T18:00:00.000+08:00_R001_2017-09-06T18:00:00.000+08:00_larluo" )
  (task-ready? "2017-09-06T18:00:00.000+08:00_larluo" [zk-con zk-root] (refresh-job-spec "2017-09-06T18:00:00.000+08:00_larluo" [zk-con zk-root]))
  )
