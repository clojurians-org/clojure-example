(ns spark-etl.expr
  (:require [flambo.conf :as conf]
            [flambo.api :as f :refer [defsparkfn]]
            [flambo.tuple :as ft]
            [incanter.core :refer [sel to-matrix to-list]]
            [incanter.io :refer [read-dataset]]
            [clojure.data.csv :as csv :refer [read-csv write-csv]]
            [clojure.java.io :as io]
            [infix.macros :refer [infix from-string]]
            [clj-time.local :refer [format-local-time to-local-date-time]]
            [clj-time.periodic :refer [periodic-seq]])
  (:import [java.net URI]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path])
  (:gen-class))

(defn parse-date-seq "2013-01-01:2013-01-03 => [2013-01-01, 2013-01-02, 2013-01-03]"
  [date-range-str]
  (let [date-range (clojure.string/split date-range-str #":" 2)]
    (as-> date-range $
      (mapv to-local-date-time $)
      (conj $ (clj-time.core/days 1))
      (apply periodic-seq $)
      (mapv #(format-local-time % :date) $)
      (conj $ (second date-range)))))

(defn expr-fn [expr]
  (let [params (as-> expr $
                 (re-seq #":\b(\w+)\b" $) (mapv second $)
                 (concat ["["] $ ["]"])
                 (clojure.string/join " " $))]

    (binding [*ns* (find-ns 'spark-etl.expr)]
      (read-string (str "(fn [{:keys " params "}] (infix " (clojure.string/replace expr #":" "") "))")))))

((expr-fn ":copon_discount_amount > 0") {:pay_date "2016-02-01", :barcode "2015120800012", :tax_amount "0.0", :preview_show_id "0", :logistics_amount "0.0", :event_id "0", :order_id "145430262841851", :show_id "0", :user_id "145347431764454", :coupon_id "NULL", :warehouse_id "18", :quantity "1", :system_discount_amount "8.8", :price "88.0", :replay_show_id "0", :copon_discount_amount -1})

(defn resolve-expr
  "resolve single expression recursively"
  [expr-name result-data-map expr-map]
  (prn "expr-name:" expr-name "result-data-map:" result-data-map "expr-map:" expr-map)
  (when-not (contains? @result-data-map expr-name)
    (doseq [ sub-expr (->> expr-name expr-map (re-seq #":\b(\w+)\b") (mapv (comp keyword second)))]
      (resolve-expr sub-expr result-data-map expr-map))
    (prn "expr-map:" (->> expr-name expr-map) @result-data-map)
    (swap! result-data-map assoc expr-name ((expr-fn (->> expr-name expr-map)) @result-data-map))))

(defn expand-expr-map
  "calculate out-cols according data-map and expr-map"
  [out-cols data-map expr-map]
  (let [result-data-map (atom data-map)]
    (doseq [each-expr-name (keys expr-map)]
      (when-not (contains? @result-data-map each-expr-name) (resolve-expr each-expr-name result-data-map expr-map)))
    (map @result-data-map out-cols)))

(defsparkfn do-expr-map
  [record tab-cols expr-map]
  (prn "record: " record "tab-cols:" tab-cols "expr-map: " expr-map)
  (let [data-map (->> (clojure.string/split record #"\001") (interleave (map keyword tab-cols)) (apply hash-map))
        out-cols (concat (->> tab-cols (filter keyword?))  (-> expr-map keys))]
    (clojure.string/join "\001" (expand-expr-map out-cols data-map expr-map))))

(defn -main [date-range-str data-source-str expr-map-str]
  (let [hive-dw-dir "hdfs://192.168.1.3:9000/user/hive/warehouse"
        partition-part (str "p_date={" (->> date-range-str parse-date-seq (clojure.string/join ",")) "}")
        [schema-tab tab-cols] (read-string data-source-str)
        [schema tab] (clojure.string/split (name schema-tab) #"\." 2)
        expr-map (read-string expr-map-str)
        src-path (clojure.string/join "/" [hive-dw-dir (str schema ".db") tab partition-part])
        conf (-> (conf/spark-conf) (conf/app-name (str "expr-" schema "-" tab)))
        conf (cond-> conf (not (.get conf "spark.master" nil)) (conf/master "local[*]"))]
    (defonce sc (f/spark-context conf))
    (-> sc .hadoopConfiguration (.set "mapreduce.input.fileinputformat.input.dir.recursive" "true"))
    (-> sc
        (f/text-file src-path)
        (f/map (f/fn [line] (do-expr-map line tab-cols expr-map)))
        f/first)))

(comment
  (-main "2016-02-01:2016-02-01"
                  "[:ods.d_bolome_orders
           [:pay_date :user_id :order_id \"barcode\" 
            :quantity :price :warehouse_id :show_id 
            :preview_show_id :replay_show_id :coupon_id :event_id 
            :copon_discount_amount :system_discount_amount :tax_amount :logistics_amount]]"
                           "{:is_copon_discount \":copon_discount_amount > 0\"
           :is_system_discount \":system_discount_amount > 0\"
           :is_discount \":is_copon_discount || :is_system_discount\"}"))


(def hypot
  (from-string [x y]
               "sqrt(x**2 + y**2)"))

(hypot 3 "4")
