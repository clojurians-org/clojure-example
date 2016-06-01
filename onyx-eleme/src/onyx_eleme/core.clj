(ns onyx-eleme.core
  (:require [cheshire.core :as cheshire]
            [clj-http.client :as http]
            [clojure.core.async :as async :refer [>!! <!! timeout chan close!]]
            [taoensso.timbre :as timbre]
            [onyx.plugin.core-async]
            [onyx.plugin.datomic]
            [onyx.plugin.redis]
            [onyx.api]
            [dire.core :as dire]
            [datomic.api :as datomic]
            [clojure.string :as str]
            [onyx.static.planning]))

;;;;;;;;;;;;;;;;;;;
;; FUNCTION
;;;;;;;;;;;;;;;;;;
(def workflow [[:read-datoms   :eleme-http]
               [:eleme-http    :redis-prepare]
               [:redis-prepare :out-to-redis]])

(defn eleme-http [{:keys [datoms] :as segment}]
   (when-first [restaurant-id  (map #(nth % 2) (filter #(= (second %) :restaurant/id) datoms)) ]
     (timbre/info "segment:" segment "id:" restaurant-id)
     {:n restaurant-id
      :json (:body (http/get (str "https://www.ele.me/restapi/v4/restaurants/" restaurant-id)  {:insecure? true}))} ))

(dire/with-handler! #'eleme-http
  [:status 404]
  (fn [e segment]
    (timbre/error "eleme-http[404]: " segment)
    []))

(dire/with-handler! #'eleme-http
  [:status 502]
  (fn [e segment]
    #_(timbre/error "eleme-http[502]: " segment "-> sleep for 0.1 seconds")
    #_(Thread/sleep 100)
    (dire/supervise #'eleme-http segment)))

(dire/with-handler! #'eleme-http
  [:status 500]
  (fn [e segment]
    (timbre/error "eleme-http[500]: " segment "-> sleep for 0.1 seconds")
    (Thread/sleep 100)
    (dire/supervise #'eleme-http segment)))
(defn redis-prepare [{:keys [n json] :as segment}]
  #_(timbre/debug segment "@segment")
  (if (seq segment)  {:op :set :args [(str/join ":" ["eleme" n "json"]) (str json ) ] } []))

;;;;;;;;;;;;;;
;; setup env
;;;;;;;;;;;;;
(def id (java.util.UUID/randomUUID))
(def env-config
  {:zookeeper/address "127.0.0.1:4188"
   :zookeeper/server? true
   :zookeeper.server/port 4188
   :onyx.bookeeper/server? true
   :onyx.bookeeper/local-quorum? true
   :onyx.bookeeper/local-quorum-ports [4196 4197 4198]
   :onyx/tenancy-id id
   :onyx.log/config {}})

(def peer-config
  {:zookeeper/address "127.0.0.1:4188"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx.log/config {}})

(def env (onyx.api/start-env env-config))
(def peer-group (onyx.api/start-peer-group peer-config))
(def n-peers 42 #_(count (set (mapcat identity workflow))))
(def v-peers (onyx.api/start-peers n-peers peer-group))

;;;;;;;;;;;;;;;;;
;; prepare data
;;;;;;;;;;;;;;;;;
(def datomic-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))
(datomic/create-database datomic-uri)
(def datomic-conn (datomic/connect datomic-uri))
(def datomic-schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :onyx.9now/eleme
    :db.install/_partition :db.part/db}
   {:db/id #db/id [:db.part/db]
    :db/ident :restaurant/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])
(def datomic-data
  (mapv #(do {:db/id (datomic/tempid :db.part/user) :restaurant/id %1}) (range 1 5e5)))

(datomic/transact datomic-conn datomic-schema)
(datomic/transact datomic-conn datomic-data)
(def datomic-db (datomic/db datomic-conn))
(def t (datomic/next-t datomic-db))

#_(datomic/q '[:find (count ?restaurant-id) :where [?e :restaurant/id ?restaurant-id]] datomic-db)
;;;;;;;;;;;;;;;;;;
;; catalog
;;;;;;;;;;;;;;;;
(def batch-size 10)
(def catalog
  [{:onyx/name :read-datoms
    :onyx/plugin :onyx.plugin.datomic/read-datoms
    :onyx/type :input
    :onyx/medium :datomic
    :datomic/uri datomic-uri
    :datomic/t t
    :datomic/partition :onyx.9now/eleme
    :datomic/datoms-index :eavt
    :datomic/datoms-per-segment 1
    :onyx/max-peers 1
    :onyx/pending-timeout (* 1000 60 5)
    :onyx/batch-size batch-size
    :onyx/doc "Reads a sequence of datoms from the d/datoms API"}
   {:onyx/name :eleme-http
    :onyx/fn ::eleme-http
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/max-peers 20
    :onyx/doc "get the eleme data from the rest api"}
   {:onyx/name :redis-prepare
    :onyx/fn ::redis-prepare
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/max-peers 20}
   {:onyx/name :out-to-redis
    :onyx/plugin :onyx.plugin.redis/writer
    :onyx/type :output
    :onyx/medium :redis
    :redis/uri "redis://127.0.0.1:6379"
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "write data to file system"}])

;;;;;;;;;;;;;;;;;;;;;;;;
;; WORKFLOW + LIFECYCLE
;;;;;;;;;;;;;;;;;;;;;;;;
(def job-id
  (:job-id
     (onyx.api/submit-job
         peer-config
         {:catalog catalog 
          :workflow workflow
          :lifecycles [{:lifecycle/task :read-datoms :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}]
          :task-scheduler :onyx.task-scheduler/balanced})))

;;;;;;;;;;;;;;;;;;;;
;; CLOSE
;;;;;;;;;;;;;;;;;;;;
(onyx.api/await-job-completion peer-config job-id)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))
(onyx.api/shutdown-peer-group peer-group)
(onyx.api/shutdown-env env)
