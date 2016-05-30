(ns mysql2oracle-realtime.core
  (:require [clojure.core.async :as async :refer [>!! <!! timeout chan thread close!]]
            [clojure.java.data :as data :refer [from-java]]
            [clj-time.coerce :refer [from-sql-date from-sql-time]]
            [clj-time.format :refer [unparse formatters]]
            [clj-time.local :refer [format-local-time]]
            [java-jdbc.sql :as sql-dsl]
            [clojure.java.jdbc :refer [insert! delete! query execute!]])
  (:import [com.github.shyiko.mysql.binlog BinaryLogClient BinaryLogClient$EventListener]))

(defmethod from-java java.util.Date [instance]  (format-local-time instance :date-hour-minute-second))
(defmethod from-java java.sql.Date [instance]  (format-local-time instance :date))
(defmethod from-java java.sql.Timestamp [instance]  (format-local-time instance :date-hour-minute-second))
(defmethod from-java java.util.BitSet [instance] (str instance))
(def binlog-ch (chan 1000))

(def db {:classname "oracle.jdbc.OracleDriver"
         :subprotocol "oracle"
         :subname "thin:@10.0.146.58:1521:orcl"
         :user "qsample"
         :password "qsample91"})

(def db-cols [:openid :unionid :UserID :State :token :tokenValidTime :nickname :sex
             :language :city :province :country :headimgurl :subscribe_time :lastMsgTime :lastValidTime
             :lat :lng :posUpdateTime :Phone :CurCityId :Action :ScaneNum :GetCouponDate
             :ViewDate :SerialID :ActiveGotBit :AttenShopID :fromWay :dos :queueNum :createTime])

(defn remove-nil [cell] (condp = cell nil "nil" "" "nil" cell))
(defn sink-oracle [segment]
  (doseq [{:keys [op database table rows]} segment row rows]
    (prn [op (keyword table)  row])
    (condp = op
      :delete (delete! db :WXACCOUNTTABLE_TMP (sql-dsl/where (apply hash-map (interleave db-cols (map remove-nil row)))) )
      :insert (insert! db :WXACCOUNTTABLE_TMP db-cols (map remove-nil row)) )))

(def table-mapping  (atom {}) )
(defmulti  process-segment  #(get-in % [:header :eventType]) )
(defmethod process-segment "TABLE_MAP" [{{:keys [tableId database table]} :data}]
  (swap! table-mapping assoc tableId {:database database :table table}))
(defmethod process-segment "DELETE_ROWS" [{{:keys [tableId rows]} :data}]
  (let [{:keys [database table]} (@table-mapping tableId)]
    (when (= table "WxAccountTable")
      (sink-oracle [{:op :delete :database database :table table :rows rows}]))))
(defmethod process-segment "WRITE_ROWS" [{{:keys [tableId rows]} :data}]
  (let [{:keys [database table]} (@table-mapping tableId)]
    (when (= table "WxAccountTable")
      (sink-oracle [{:op :insert :database database :table table :rows rows}] ) ) ))
(defmethod process-segment "UPDATE_ROWS" [{{:keys [tableId rows]} :data}]
  (let [{:keys [database table]} (@table-mapping tableId)]
    (when (= table "WxAccountTable")
      (sink-oracle [{:op :delete :database database :table table :rows (map :key rows)}
                    {:op :insert :database database :table table :rows (map :value rows)} ])) ))

(defmethod process-segment :default [_])
(let [client (BinaryLogClient. "10.0.146.10" 3306 "bigdatatongbu" "bigdatatongbu")
      listener (reify BinaryLogClient$EventListener (onEvent [this event] (>!! binlog-ch (from-java event) )))
      _ (doto client (.registerEventListener listener) (.connect (* 1000 5)))]
  (while true (let [segment (<!! binlog-ch)] (process-segment segment)) ))
