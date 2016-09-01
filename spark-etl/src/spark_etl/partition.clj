(ns spark-etl.ods
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [com.rpl.specter :refer [ALL FIRST LAST submap]]
            [com.rpl.specter.macros :refer [select]]
            [incanter.core :refer [sel to-matrix to-list]]
            [incanter.io :refer [read-dataset]]
            [clojure.data.csv :as csv :refer [read-csv write-csv]]
            [clojure.java.io :as io])
  (:import [java.net URI]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path])
  (:gen-class))

(f/defsparkfn extract-partition [record partition-cols]
  (let [dataset (-> record .getBytes (read-dataset :header true :delim \,))]
    (.iterator (map ft/tuple (-> dataset (sel :cols (map last partition-cols)) to-list seq) (-> dataset to-list seq)))))

(defn write-hdfs [record tgt-path partition-cols]
  (let [[partition-vals rows] (f/untuple record)
        partition-path (clojure.string/join "/" (map #(str %1 "=" %2) (map (comp name first) partition-cols) partition-vals))
        fs (FileSystem/get (new URI tgt-path) (new Configuration))]
    (with-open [out (.create fs (new Path (clojure.string/join "/" [tgt-path partition-path "data.csv"])))]
      (doseq [row rows]
        (.write out (-> (clojure.string/join "\001" row) (.getBytes "UTF-8")))
        (.write out (.getBytes "\n" "UTF-8"))))))

(defn -main [db-tab partition-cols-str]
  (let [partition-cols (read-string partition-cols-str)
        src-path (clojure.string/join "/"  ["hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db" db-tab "*"])
        tgt-path (clojure.string/join "/"  ["hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db" db-tab])]
    (defonce sc (-> (conf/spark-conf) #_ (conf/master "local[*]") (conf/app-name (str "ods-" db-tab)) f/spark-context))
    (-> sc
        (f/whole-text-files src-path)
        .values
        (f/flat-map-to-pair (f/fn [line] (extract-partition line partition-cols)))
        (f/partition-by (f/hash-partitioner 8))
        f/group-by-key
        (f/foreach (f/fn [line] (write-hdfs line tgt-path partition-cols))))))

(comment
  (-main "d_sample_data" [[:p_date 4] [:basic_code 18]])
  (-main "d_bolome_events" "[[:p_date 3]]")
  ; lein run -m etl-helper.core d_sample_data '[[:date 4] [:basic_code 18]]'
  )
