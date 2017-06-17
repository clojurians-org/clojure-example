(ns mongo-client.core
  (:require [monger.core :as mg]
            [monger.collection :as mc]))

(let [conn (mg/connect {:host "192.168.1.2"})
      db (mg/get-db conn "monger-test")]
  (mc/insert-and-return db "documents" {:name "John" :age 30}))
