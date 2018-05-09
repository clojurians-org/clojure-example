(ns redis-ws.core)

(do
  (require '[org.httpkit.server :refer [run-server with-channel on-receive on-close send!] ])
  (import '[redis.clients.jedis JedisCluster HostAndPort])
  )

(def redis-con (new JedisCluster #{(new HostAndPort "localhost" (int 6379))}))

(defn handler [request]
  (with-channel request channel
    (on-receive channel
                (fn [data]
                  (println "start...")
                  (while true
                    (send! channel (.get redis-con "larluo-test"))
                    (Thread/sleep 1000)
                    )
                  ))
    (on-close channel (fn [status] (println "channel closed: " status)))
    ))


(defn -main []
  (run-server handler {:port 1111})
  )

(comment
  (-main)
  )

