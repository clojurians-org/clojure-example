(ns mysql-client.core
  (:require [clojure.java.jdbc :as j]))

(def mysql-db {:dbtype "mysql"
               :dbname "state_store"
               :user "state_store"
               :password "spiderdt"})

(def mysql-db2
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306/state_store"
   :user "state_store"
   :password "spiderdt"})


(j/query mysql-db2 "select * from classifieds")
(j/execute! mysql-db "delete from classifieds where rowkey = 'ddd'")



