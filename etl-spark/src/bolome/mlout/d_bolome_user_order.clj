;#*********************************
;# [intro]
;#   author=larluo@spiderdt.com
;#   func=partition algorithm for data warehouse
;#=================================
;# [param]
;#   tabname=staging table name
;#   prt_cols_str=ods partition cols
;#=================================
;# [caller]
;#   [PROG] bolome.dau
;#=================================
;# [version]
;#   v1_0=2016-09-28@larluo{create}
;#*********************************

(ns bolome.mlout.d_bolome_user_order
  (:require [powderkeg.core :as keg]
            [net.cgrand.xforms :as x]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]])
  (:import [org.apache.spark.sql SparkSession]
           [org.apache.spark.sql.types StringType StructField StructType DataTypes ]
           [org.apache.spark.sql Row SaveMode RowFactory]
           [org.apache.spark.ml.linalg Vectors VectorUDT]
           [org.apache.spark.ml.classification LogisticRegression LogisticRegressionModel RandomForestClassifier RandomForestClassificationModel]))

(defn -main []
  
  (System/exit 0)
  )

(comment
  (keg/connect! "local")
  (def ss (->> keg/*sc* .sc (new SparkSession)))
  (doto  (-> keg/*sc* .sc .conf)
    (.set "spark.app.name" "d_bolome_user_order")
    (.set "spark.master" "yarn"))
  (.close keg/*sc*)

  (def train-edn [[[0.0 1.1 0.1] 1.0]
                  [[2.0 1.0 -1.0] 0.0]
                  [[2.0 1.3 1.0] 0.0]
                  [[0.0 1.2 -0.5] 1.0]])

  (def test-edn [[[-1.0 1.5 1.3] 1.0]
                 [[3.0 2.0 -0.1] 0.0]
                 [[0.0 2.2 -1.5] 1.0]])
  (defn build-train [edn]
    (as-> (map (fn [[features label]]
                  (->> [label (Vectors/dense (double-array features))]
                       (into-array Object)
                       RowFactory/create)))
        $
      (sequence $ edn)
      (.createDataFrame (->> keg/*sc* .sc (new SparkSession)) $
                        (DataTypes/createStructType
                         [(DataTypes/createStructField "label" DataTypes/DoubleType false)
                          (DataTypes/createStructField "features" (new VectorUDT) false)]))))

  (def model (-> (new RandomForestClassifier)
                 (.setNumTrees 10)
                 (.fit (build-train train-edn))))
  
  (->> (->  model (.transform (build-train test-edn)) #_(.schema .fieldNames seq) .rdd
            (keg/rdd (map #(mapv (fn [idx] (let [obj (.get % idx)]
                                             (if (instance? org.apache.spark.ml.linalg.DenseVector obj) (-> obj .values vec) obj)))
                                 (-> % .length range)))
                     (map #(zipmap [:label :features :rawPrediction :probability :prediction] %))))
       (into [])
       first))
