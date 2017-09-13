(ns dataserver-scheduler.task-driver)

(do
  (import '[org.apache.curator.framework CuratorFrameworkFactory])
  (import '[org.apache.curator.retry ExponentialBackoffRetry])
  )

(defmulti receive (fn [source & others] source))
