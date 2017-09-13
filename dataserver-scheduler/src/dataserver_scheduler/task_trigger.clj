(ns dataserver-scheduler.task-trigger)

(do
  (require '[cheshire.core :refer [generate-string parse-string]])
  (require '[clojure.core.async :refer [chan >!! <!! close! timeout alts!! poll!]])
  
  (import '[org.apache.curator.framework CuratorFrameworkFactory]
  (import '[org.apache.curator.retry ExponentialBackoffRetry]))
  (import '[java.util.concurrent.locks ReentrantLock])

  (import '[org.joda.time DateTime])
  (import '[org.joda.time.format ISOPeriodFormat ISODateTimeFormat])
  (import '[org.joda.time Duration])
  )

(defn period-seq [ts period]
  (let [ts-obj (DateTime/parse ts)
        period-obj (.parsePeriod (ISOPeriodFormat/standard) period)]
    (letfn [(my-period-seq [ts period]
              (lazy-seq (cons ts (my-period-seq (.plus ts period) period))))]
      (my-period-seq ts-obj period-obj))))
  
(defn init [zk-con]
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/001_trigger/waiting"))
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/001_running"))
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/002_isolating"))
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/003_recovering"))
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/004_failed"))
  (-> zk-con .create .createParentsIfNeeded (.forPath "/solr5/scheduler/002_dispatcher/005_blocking/P009"))
  )

(defn realize-schedule [schedule]
  ((comp (partial (memfn print ts) (ISODateTimeFormat/dateTime)) first filter)
    (memfn isAfterNow)
    (apply period-seq ((comp rest re-find) #"R[0-9]*/(.*)/(P.*)" schedule))))

(defn calc-remaining-msecs [task-id]
  (-> (new Duration (DateTime/now) (DateTime/parse ((comp first clojure.string/split) task-id #"_"))) .getMillis))

(defn create-job [job-id [zk-con zk-root] [job-specs job-args]]
  (let [specs-zpath (format "%s/configs/%s/specs.json" zk-root job-id)
        args-zpath (format "%s/configs/%s/args.json" zk-root job-id)]
    (-> zk-con .create .creatingParentsIfNeeded (.forPath specs-zpath))
    (-> zk-con .setData (.forPath specs-zpath (-> job-specs (generate-string {:pretty true}) .getBytes)))
    (when job-args
      (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/configs/%s/args.json" zk-root job-id)))
      (-> zk-con .setData (.forPath args-zpath (-> job-args (generate-string {:pretty true}) .getBytes))))))

(defn load-job [job-id [zk-con zk-root]]
  (let [job-spec (-> zk-con .getData (.forPath (format "%s/configs/%s/specs.json" zk-root job-id)) String. parse-string)
        coming-schedule (realize-schedule (get job-spec "schedule"))
        job-zpath (format "%s/living_jobs/%s" zk-root job-id)
        waiting-zpath (format "%s/001_trigger/waiting" zk-root)]
    (when-not (-> zk-con .checkExists (.forPath (format "%s/state.json" job-zpath)))
      (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/state.json" job-zpath))))
    (when-not (-> zk-con .checkExists (.forPath (format "%s/metrics.json" job-zpath)))
      (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/metrics.json" job-zpath))))
    (when-not (-> zk-con .checkExists (.forPath (format "%s/tasks/schedule/%s_%s_waiting" job-zpath coming-schedule job-id)))
      (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/%s_%s" waiting-zpath coming-schedule job-id)))
    (when-not (-> zk-con .checkExists (.forPath (format "%s/%s_%s" waiting-zpath coming-schedule job-id)))
      (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/%s_%s" waiting-zpath coming-schedule job-id)))))))

(comment
  (def zk-url "")
  (def zk-con (doto (CuratorFrameworkFactory/newClient zk-url (new ExponentialBackoffRetry 10000 60)) .start))
  (-> zk-con .delete (.forPath "/solr5/scheduler/configs/larluo/specs.json"))
  (create-job "larluo" [zk-con "/solr5/scheduler"]
    [{:schedule "R1/2017-09-01T10:00:00+0800/PT2H"
      :command "ipconfig"
      :description "This is a test from larluo!"
      :priority 5
      :resource {:cost nil
                 :mem nil
                 :disk nil}
      :retry {:backoff-sec 10
              :warning-threshold 10}
      :owner "hao1.luo@spdbccc.com.cn"
      :watcher "hao1.luo@spdbccc.com.cn"}])
  (load-job "larluo" [zk-con "/solr5/scheduler"])
  )

(defn load-jobs-if [[zk-con zk-root]]
  (let [configs-zpath (format "%s/configs" zk-root)
        jobs-zpath (format "%s/living_jobs" zk-root)]
    (when-not (seq (-> zk-con .getChildren (.forPath jobs-zpath)))
      ((comp dorun sequence) (map #(load-job % [zk-con zk-root])) (-> zk-con .getChildren (.forPath configs-zpath))))))

(defn locate-coming-task [[zk-con zk-root]]
  (some-> zk-con .getChildren (.forPath (format "%s/001_trigger/waiting" zk-root)) sort first))

(defn refresh-job-spec [task-id [zk-con zk-root]]
  (-> zk-con .getData (.forPath (format "%s/configs/%s/specs.json" zk-root (subs task-id 30))) String. (parse-string true)))

(defn calc-next-period [task-id job-spec]
  (let [period-obj (-> job-spec :schedule ((comp last clojure.string/split) #"/") (->> (.parsePeriod (ISOPeriodFormat/standard))))
        [ts job-id] (clojure.string/split task-id #"_" 2)]
    (-> (.plus (DateTime/parse ts) period-oj)
        (->> ((memfn print ts-obj) (ISODateTimeFormat/dateTime)))
        ((partial format "%s_%s") job-id))))

(defn event-loop [event-chan [ack-chain [zk-con zk-root]]]
  (some nil? (repeatedly #(poll! event-chain)))
  (loop []
    (println [:fn :event-loop :loop-start :dt (new java.util.Date)])
    (if-let [coming-task (locate-coming-task [zk-con zk-root])]
      (let [remaining-msecs (calc-remaining-msecs coming-task)
            coming-job-spec (refresh-job-spec coming-task [zk-con zk-root])]
        (println [:fn :event-loop :coming-task coming-task :remaining-msecs remaining-msecs :coming-job-spec coming-job-spec] )
        (if (<= remaing-msecs 0)
          (do (println "hello1")
              (ack-next :trigger [ack-chain [zk-con zk-root]] [:dispatch-task coming-task coming-job-spec])
              (println "hello2")
              (-> zk-con .create .creatingParentsIfNeeded (.forPath (format "%s/001_trigger/waiting/%s" zk-root (calc-next-period coming-task coming-job-spec))))
              (-> zk-con .delete (.forPath (format "%s/001_trigger/waiting/%s" zk-root coming-task))))
          (let [[v ch] (alts!! [event-chan (timeout remaining-msecs)])]
            (when-not (and (= ch event-chan) (= :done (<!! event-chan))) (recur)))))
    (when (not= :done (<!! event-chan)) (recur)))))

(defn event-loop-daemon (memoize (fn [& args] (future (try (apply event-loop args) (catch Exception e (prn e)))))))
(defmulti receive-event (fn [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]] [event-source event-type]))
(defonce receive-lck (new Object))
(defonce *event-chan* (chan 10))

(defn receive [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  (locking receive-lck
    (try (receive-event [event-soure event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id])
      (finally (-> zk-con .delete (.forPath (format "%s/channels/%s/from_%s/%016d_%s" zk-root (name event-target) (name event-source message-id event-id))))))))

(defmethod receive-event [:overseer :start-task-consumer]
  [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  (println [:oversser :start-task-consumer])
  (event-loop-daemon *event-chan* [ack-chain [zk-con zk-root]]))

(defmethod receive-event [:overseer :stop-task-consumer]
  [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  (println [:overseer :ping])
  (>!! *event-chan* :ping))

(defmethod receive-event [:dispatcher :recovery-task]
  [[event-source event-target] [ack-chain [zk-con zk-root]] [message-id event-type event-id]]
  )
