(ns thrift-storm-ui.core)

(import '[org.apache.storm.thrift.protocol TBinaryProtocol TProtocol])                                                     
(import '[org.apache.storm.thrift.transport TTransport TFramedTransport TSocket])                                          
(import '[org.apache.storm.generated Nimbus$Client])                                                                       
(use '[slingshot.slingshot :refer [try+ throw+]])                                                                          
                                                                                                                           
(defmacro with-nimbus-thrift [[client {:keys [host port] :or {port 6627}}] & body]                                         
  `(let [transport# (TFramedTransport. (TSocket. ~host ~port))                                                             
         protocol#  (TBinaryProtocol. transport#)                                                                          
         ~client    (Nimbus$Client. protocol#)]                                                                            
     (.open transport#) (try+ ~@body (finally (.close transport#)))))                                                      
                                                                                                                           
(with-nimbus-thrift [client {:host "localhost"}]                                                                           
  (->> client .getClusterInfo .get_supervisors)                                                                            
  )
