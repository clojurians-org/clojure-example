(ns cancelable-wait.core)

(defn sleep-for [latch-atom timeout-ms]                                                                                                                                                 
  (println [:sleep-for-start] )                                                                                                                                                         
  (.await @latch-atom timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)                                                                                                            
  (reset! latch-atom (new java.util.concurrent.CountDownLatch 1))                                                                                                                       
  (println [:sleep-for-end] )                                                                                                                                                           
)
(comment
  (def latch-atom (atom (new java.util.concurrent.CountDownLatch 1)))
  (future (sleep-for latch-atom (* 1000 10)))
  (future (sleep-for latch-atom (* 1000 60 60)))
  (swap! latch-atom #(.countDown %1))
  )


  
  

