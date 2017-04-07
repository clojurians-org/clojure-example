(ns bolome.ods.d_bolome_product_category
  (:require [powderkeg.core :as keg]
            [net.cgrand.xforms :as x]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.matrix :as m]
            [clojure.java.jdbc :as j]
            [cheshire.core :refer [generate-string parse-stream parse-string]]
            [clojure.java.io :as io])
  (:import [org.apache.spark SparkFiles]
           [org.apache.spark TaskContext]
           [org.apache.spark.rdd JdbcRDD$ConnectionFactory]
           [org.apache.spark.sql SparkSession]
           [org.apache.spark.sql.types StringType StructField StructType]
           [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.ml.linalg Vectors VectorUDT]
           [org.apache.spark.sql Row SaveMode RowFactory]
           [org.apache.spark.rdd JdbcRDD]))

(defn group-by-index [xs idxs]
  (mapv (partial mapv xs) (conj idxs (vec (remove #(contains? (->> idxs (mapcat identity) set) %) (-> xs count range))))) )

(def pg-spec {:dbtype "postgresql"
              :dbname "dw"
              :host "192.168.1.3"
              :user "ms"
              :password "spiderdt"
              :sslmode "require"
              :sslkey (-> "postgres/client.key.pk8" io/resource .getPath)
              :sslcert (-> "postgres/client.cert.pem" io/resource .getPath)
              :sslrootcert (-> "postgres/root.cert.pem" io/resource .getPath)
              :sslfactory "org.postgresql.ssl.jdbc4.LibPQFactory"
              :rewriteBatchedStatements "true" })
(defn latest-stg-d_bolome_product_category []
  (as-> (JdbcRDD/create keg/*sc*
                        (reify org.apache.spark.rdd.JdbcRDD$ConnectionFactory
                          (getConnection [this] (j/get-connection pg-spec)))
                        "SELECT * FROM stg.d_bolome_product_category where ? = 0 and ? = 0"
                        (long 0) (long 0) (int 1))
      $
    (keg/rdd $
             (map vec)
             (map #(group-by-index % [[0] []]))
             (map (juxt first (comp vec next)))
             #_(take 10))))

(defn latest-ods-d_bolome_product_category []
  (as-> (JdbcRDD/create keg/*sc*
                        (reify org.apache.spark.rdd.JdbcRDD$ConnectionFactory
                          (getConnection [this] (j/get-connection pg-spec)))
                        "SELECT * FROM ods.d_bolome_product_category where ? = 0 and ? = 0"
                        (long 0) (long 0) (int 1))
      $
    (keg/rdd $
             (map vec)
             (map #(group-by-index % [[3] [0 1 2]]))
             (map (juxt (comp clojure.edn/read-string ffirst) (comp vec next)))
             #_(take 10))))

(defn stat-dw-id
  ([] [0 (sorted-map)])
  ([acc x]
   (if (map? (second x))
     (let [[acc-dw-id-max acc-new-pr-dw-id-cnt] acc
           [dw-id-max new-pr-dw-id-cnt] x]
       [(max acc-dw-id-max dw-id-max) (merge-with + acc-new-pr-dw-id-cnt new-pr-dw-id-cnt)])
     (let [[acc-dw-id-max acc-new-pr-dw-id-cnt] acc
           [pr-no stg-dw-id ods-dw-id] x]
       [(->> [acc-dw-id-max ods-dw-id] (keep identity) (apply max 0))
        (update acc-new-pr-dw-id-cnt pr-no #(cond-> (or % 0) (not ods-dw-id) inc))] )))
  ([x] x))

(defn filter-indexed
  ([pred]
   (fn [rf]
     (let [i (volatile! (dec 0))]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (if (pred input) (rf result [(vswap! i inc) input]) (rf result [nil input]))))))))

(defn offset-dw-id [[dw-id-max new-pr-dw-id-cnt]]
  (reduce (fn [acc [k v]] (assoc acc (inc k) (apply + v (vals acc) ))) {0 (inc dw-id-max)} new-pr-dw-id-cnt))

(defn -main []
  (prn {:spark-files (SparkFiles/getRootDirectory)})

  #_(into [] merge-d_bolome_product_category)

  (def ids-d_bolome_product_category
    (let [merge-d_bolome_product_category  (->> (keg/join (keg/by-key (latest-stg-d_bolome_product_category)) :or nil
                                                          (keg/by-key (latest-ods-d_bolome_product_category)) :or nil)
                                                .cache)
          dw-id-offset  (->> (keg/rdd merge-d_bolome_product_category
                                      (map (fn [[dw-src-id [[[stg-dw-id stg-dw-first-ts stg-dw-latest-ts] stg-row]
                                                            [[ods-dw-id ods-dw-first-ts ods-dw-latest-ts] ods-row]]]]
                                             [(.. TaskContext (get) (partitionId)) stg-dw-id ods-dw-id]))
                                      (x/reduce stat-dw-id))
                             (into [])
                             (into [] (x/reduce stat-dw-id))
                             first
                             offset-dw-id)]
      (j/execute! pg-spec "TRUNCATE TABLE ods.d_bolome_product_category")
      (-> (keg/rdd merge-d_bolome_product_category
                    (filter-indexed (fn [[dw-src-id [[[stg-dw-id stg-dw-first-ts stg-dw-latest-ts] stg-row]
                                                     [[ods-dw-id ods-dw-first-ts ods-dw-latest-ts] ods-row]]]]
                                      (nil? ods-dw-id)))
                    (map (fn [[new-dw-id [dw-src-id [[[stg-dw-id stg-dw-first-ts stg-dw-latest-ts] stg-row]
                                                     [[ods-dw-id ods-dw-first-ts ods-dw-latest-ts] ods-row]]]]]
                           (vec (list* (if new-dw-id (+ new-dw-id (dw-id-offset (.. TaskContext (get) (partitionId)))) ods-dw-id)
                                       (or stg-dw-first-ts ods-dw-first-ts "")
                                       (or ods-dw-latest-ts stg-dw-latest-ts "")
                                       (pr-str dw-src-id)
                                       (or stg-row ods-row))))))
          (keg/do-rdd
           [db-spec pg-spec
            out-cols [:dw_id :dw_first_ts :dw_latest_ts :dw_src_id
                      :product_name :category_1 :category_2]]
           (partition-by (constantly true))
           (map (fn [rows]
                  (j/insert-multi! pg-spec :ods.d_bolome_product_category
                                   out-cols rows)) ))
          ))
    )
)

(comment
  (keg/connect! "local")
  
  
  )

