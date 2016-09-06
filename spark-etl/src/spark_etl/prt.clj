(ns spark-etl.prt
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clojure.data.csv :as csv :refer [read-csv write-csv]]
            [clojure.java.io :as io])
  (:import [java.net URI]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path]
           [org.apache.spark TaskContext])
  (:gen-class))

(defn extract-prt [record-seq prt-cols]
  (let [prt-names (map (comp name first) prt-cols)
        prt-val-idxs (map second prt-cols)]
    #_(println "prt-names:" prt-names "prt-val-idxs: " prt-val-idxs)
    (for [record record-seq]
      (let [prt-vals (map record prt-val-idxs)
            prt-path (clojure.string/join "/" (map #(str %1 "=" %2) prt-names prt-vals)) ]
        [prt-path (cons prt-path record)]) )) )

(defn write-hdfs [key-record-itr tgt-path]
  (let [writers (atom {})]
    (doseq [[prt-path record] (iterator-seq key-record-itr)]
      (let [prt-filename (str "data.csv." (some-> (TaskContext/get) .partitionId))
            file-path (clojure.string/join "/" [tgt-path prt-path prt-filename]) ]
        (when-not ((keyword file-path) @writers )
          (swap! writers assoc (keyword file-path)
                 (-> (FileSystem/get (new URI file-path) (new Configuration))
                     (.create (new Path file-path)) )))
        (let [out (get @writers (keyword file-path))]
          (.write out (.getBytes (str (clojure.string/join "\001" record) "\n") "UTF-8"))) ) )
    (doseq [[_ writer] @writers] (.close writer))) )

(defn -main [db-tab prt-cols-str]
  (let [prt-cols (read-string prt-cols-str)
        src-path (clojure.string/join "/"  ["hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db" db-tab])
        tgt-path (clojure.string/join "/"  ["hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db" db-tab])
        conf (-> (conf/spark-conf) (conf/app-name (str "partition-" db-tab)))
        conf (cond-> conf (not (.get conf "spark.master" nil)) (conf/master "local[*]"))]
    (defonce sc (f/spark-context conf))
    (doto (.hadoopConfiguration sc)
      (.set "mapreduce.input.fileinputformat.input.dir.recursive" "true") )
    (-> sc
        (f/whole-text-files src-path 8)
        (f/flat-map (f/fn [key-text]
                      (let [[key text] (f/untuple key-text)]
                        (.iterator (extract-prt (as-> text $
                                                  (clojure.string/split $ #"\n")
                                                  (rest $)
                                                  (map #(clojure.string/split % #",") $))
                                                prt-cols)) )))
        #_(f/repartition 4)
        (f/foreach-partition (f/fn [key-record-itr] (write-hdfs key-record-itr tgt-path))))))

(comment
  (-main "d_bolome_events" "[[:p_date 3]]")
  (-main "d_bolome_orders" "[[:p_date 0]]")
  ; lein run -m etl-helper.core d_sample_data '[[:date 4] [:basic_code 18]]'
  )
