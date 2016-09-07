(ns spark-etl.expr
  (:require [flambo.conf :as conf]
            [flambo.api :as f :refer [defsparkfn]]
            [flambo.tuple :as ft]
            [clojure.data.csv :as csv :refer [read-csv write-csv]]
            [clojure.java.io :as io]
            [infix.macros :refer [infix from-string]]
            [clj-time.local :refer [format-local-time to-local-date-time]]
            [clj-time.periodic :refer [periodic-seq]])
  (:import [java.net URI]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.hdfs DistributedFileSystem]
           [org.apache.spark TaskContext])
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
      (load-string (str "(fn [{:keys " params "}] (infix " (clojure.string/replace expr #":" "") "))")))))

(defn resolve-expr
  "resolve single expression recursively"
  [expr-name result-data-map expr-map]
  (when-not (contains? @result-data-map expr-name)
    (doseq [ sub-expr (->> expr-name expr-map (re-seq #":\b(\w+)\b") (mapv (comp keyword second)))]
      (resolve-expr sub-expr result-data-map expr-map))
    (swap! result-data-map assoc expr-name ((expr-fn (->> expr-name expr-map)) @result-data-map))))

(defn expand-expr-map
  "calculate out-cols according data-map and expr-map"
  [out-cols data-map expr-map]
  (let [result-data-map (atom data-map)]
    (doseq [each-expr-name (keys expr-map)]
      (when-not (contains? @result-data-map each-expr-name)
        (resolve-expr each-expr-name result-data-map expr-map)))
    (map @result-data-map out-cols)))

(defn do-expr-map
  [record tab-cols-str expr-map]
  (let [data-map (->> record
                      (map
                       #(if (symbol? %1) [(keyword %1) (read-string %2)] [((comp keyword eval) %1) %2])
                       (-> tab-cols-str (clojure.string/replace #"'?\b_" "") read-string) )
                      (into {}))
        out-cols (concat (as-> tab-cols-str $
                           (clojure.string/replace $ #"'?\b_\w+\b" "")
                           (read-string $)
                           (map #(-> (if-not (symbol? %) (eval %) %) keyword) $))
                         (-> expr-map keys))]
    [(first record) (expand-expr-map out-cols data-map expr-map) ] ))

(defn write-hdfs [key-record-itr tgt-path]
  (let [prt-id (some-> (TaskContext/get) .partitionId)
        prt-filename (str "data.csv." prt-id)
        counter (atom 0)
        writers (atom {})]
    (doseq [[prt-path record] (iterator-seq key-record-itr)]
      (let [file-path (clojure.string/join "/" [tgt-path prt-path prt-filename])]
        (when-not ((keyword file-path) @writers)
          (swap! writers assoc (keyword file-path)
                 (-> (FileSystem/get (new URI file-path) (new Configuration))
                     (.create (new Path file-path)))))
        (let [out (get @writers (keyword file-path))]
          (println prt-id "-counter:" (swap! counter inc))
          (.write out (.getBytes (str (clojure.string/join "\001" record) "\n") "UTF-8")))))
    (doseq [[_ writer] @writers] (.close writer))))

(defn -main [tab-names-str tab-cols-str expr-map-str]
  (infix.core/suppress! 'e)
  (let [hive-dw-dir "hdfs://192.168.1.3:9000/user/hive/warehouse"
        expr-map (read-string expr-map-str)
        ;partition-part (str "p_date={" (->> date-range-str parse-date-seq (clojure.string/join ",")) "}")
        [src-tab-name tgt-tab-name] (read-string tab-names-str) 
        [src-schema src-tab] (clojure.string/split (name src-tab-name) #"\." 2)
        [tgt-schema tgt-tab] (clojure.string/split (name tgt-tab-name) #"\." 2)
        src-path-part (clojure.string/join "/" [(str src-schema ".db") src-tab])
        tgt-path-part (clojure.string/join "/" [(str tgt-schema ".db") tgt-tab])
        conf (-> (conf/spark-conf) (conf/app-name (str "expr-" tgt-schema "-" tgt-tab)))
        conf (cond-> conf (not (.get conf "spark.master" nil)) (conf/master "local[*]"))]
    (defonce sc (f/spark-context conf))
    (-> sc .hadoopConfiguration (.set "mapreduce.input.fileinputformat.input.dir.recursive" "true"))
    (-> sc
        (f/whole-text-files (clojure.string/join "/" [hive-dw-dir src-path-part]))
        (f/flat-map (f/fn [key-text] (let [[key text] (f/untuple key-text)]
                                       (as-> text $
                                         (clojure.string/split $ #"\n")
                                         (map #(-> % (clojure.string/split #"\001") #_(do-expr-map tab-cols-str expr-map)) $)
                                         (.iterator $) ))))
        f/count
        #_(f/foreach-partition (f/fn [key-line-itr] (write-hdfs key-line-itr (clojure.string/join "/" [hive-dw-dir tgt-path-part])))) )))

(comment
  (-main "[:ods.d_bolome_orders :4ml.d_bolome_orders]"
         "['_prt_path
           'pay_date '_user_id 'order_id 'barcode
            'quantity 'price 'warehouse_id 'show_id 
            'preview_show_id 'replay_show_id 'coupon_id 'event_id 
            copon_discount_amount system_discount_amount 'tax_amount 'logistics_amount]"
         "{:is_copon_discount \":copon_discount_amount == 0\"
           :is_system_discount \":system_discount_amount == 0.0\"
           :is_discount \":is_copon_discount || :is_system_discount\"}"))

(time (dotimes [_ 1000000] (infix 1 + 2)))
