(ns hive-client.core
  (:require [clojure.java.jdbc :as j]
            [taoensso.timbre :refer [info debug warn set-level!]])
  (:import [org.apache.thrift.transport TSocket TSaslClientTransport]
           [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.hive.service.rpc.thrift
            TCLIService$Client TProtocolVersion TOpenSessionReq TFetchResultsReq
            TGetSchemasReq TExecuteStatementReq TFetchOrientation]
           [org.apache.hive.service.auth PlainSaslHelper$PlainCallbackHandler]))

(set-level! :trace) 
(def hive-db {:classname "org.apache.hive.jdbc.HiveDriver"
              :subname "//192.168.1.2:10000/ods"
               :subprotocol "hive2"})

(comment
  (j/query hive-db "select * from  hbase.d_sample_data limit 10")
  (j/query hive-db "select * from hbase.d_sample_data")
  (j/query hive-db "select count(*) from hbase.d_sample_dat")
  (j/execute! hive-db "add jar *.jar")

  (with-open [transport (->> (new TSocket "192.168.1.2" 10000)
                             (new TSaslClientTransport "PLAIN" nil nil nil {}
                                  (new PlainSaslHelper$PlainCallbackHandler "spiderdt" "spiderdt"))) ]
    (.open transport)
    (let [client (->> (new TBinaryProtocol transport) (new TCLIService$Client))
          sess-handler (->> (new TOpenSessionReq) (.OpenSession client) .getSessionHandle)
          op-handle (->> (new TGetSchemasReq sess-handler) (.GetSchemas client) .getOperationHandle)]
      (->> (new TFetchResultsReq op-handle TFetchOrientation/FETCH_NEXT 10) (.FetchResults client))
      ))
)
