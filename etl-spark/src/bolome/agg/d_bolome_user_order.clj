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
;#   [PORG] bolome.dau
;#   [PORG] bolome.event
;#   [PORG] bolome.inventory
;#   [PORG] bolome.order
;#   [PORG] bolome.product_category
;#   [PORG] bolome.show
;#=================================
;# [version]
;#   v1_0=2016-09-28@larluo{create}
;#*********************************

(ns bolome.agg.d_bolome_user_order
  (:require [powderkeg.core :as keg]
            [net.cgrand.xforms :as x]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]])
  (:import [org.apache.spark.api.java JavaPairRDD]
           [org.apache.spark.sql SparkSession]
           [org.apache.spark.sql.types StringType StructField StructType]
           [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Row SaveMode RowFactory]))

(def tab-trgx
  {:dp_bolome_order
   {:DATA {:fields [:order-dw-dt :order-dw-ts :order-dw-src-id
                    :product-dw-id :product-dw-src-id
                    :show-dw-id :show-dw-src-id
                    :preview-show-dw-id :preview-show-dw-src-id
                    :replay-show-dw-id :replay-show-dw-src-id
                    :pay-dt :user-id :order-id
                    :order-item-quantity :order-item-price :order-item-warehouse-id :coupon-id :event-dw-src-id
                    :coupon-discount-amount :order-item-system-discount-amount :order-item-tax-amount :order-item-logistics-amount]}
    :BRANCHS {:product  {:d_bolome_product_category
                        {:DATA {:fields [:dw-id :dw-src-id
                                         :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                         :product-category-1-dw-id :product-category-1-dw-src-id :product-category-2-dw-id :product-category-2-dw-src-id
                                         :barcode :product-name]} 
                         :BRANCHS {}}}
             :event {:d_bolome_event
                     {:DATA {:fields [:dw-dt :dw-ts :dw-src-id
                                      :event-id :type-name :event-name :create-dt]
                             :partitions [:p_dw_dt]}
                      :BRANCHS {}}}
             :show {:d_bolome_show
                    {:DATA {:fields [:dw-id :dw-src-id
                                     :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                     :show-id :show-name :begin-ts :end-ts]}
                     :BRANCHS {}}}
             :preview-show {:d_bolome_show
                            {:DATA {:fields [:dw-id :dw-src-id
                                             :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                             :show-id :show-name :begin-ts :end-ts]}
                             :BRANCHS {}}}
             :replay-show {:d_bolome_show
                            {:DATA {:fields [:dw-id :dw-src-id
                                             :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                             :show-id :show-name :begin-ts :end-ts]}
                             :BRANCHS {}}} }}})


(defn init-rdd [[node-name {{partitions :partitions fields :fields} :DATA :as node-val}]]
  (let [hive-path "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db"]
    [node-name
     (-> (assoc node-val
                :RESULT
                (keg/rdd (if partitions
                           (keg/rdd (.wholeTextFiles keg/*sc* (->> "*" (repeat (count partitions)) (concat [hive-path (name node-name)]) (clojure.string/join "/")))
                                    (map second)
                                    (mapcat #(clojure.string/split % #"\n")))
                           (.textFile keg/*sc* (str hive-path "/" (name node-name))))
                         (map #(clojure.string/split % #"\001"))
                         (take 2))
                :RESULT-FIELDS
                fields))]))

(defn rdd-join [[node-1-name {{node-1-fields :fields} :DATA rdd-1 :RESULT rdd-1-columns :RESULT-COLUMNS :as node-1-val} :as node-1]
                branch-name
                [node-2-name {{node-2-fields :fields} :DATA rdd-2 :RESULT rdd-2-columns :RESULT-COLUMNS} :as node-2]                ]
  (let [prefix-branch-field (fn [branch-name field] (->> [branch-name field] (map name) (clojure.string/join "-") keyword))
        branch-node-2-fields (map (partial prefix-branch-field branch-name) node-2-fields)
        jfs (->> (apply clojure.set/intersection (map set [node-1-fields (remove #(= (prefix-branch-field branch-name :dw-src-id) %) branch-node-2-fields)])))
        key-rdd-1 (keg/rdd rdd-1
                              (map #(vector (->> jfs (map (fn [jf] (.indexOf node-1-fields jf))) (mapv %)) %) ))
        key-rdd-2 (keg/rdd rdd-2
                              (map #(vector (->> jfs (map (fn [jf] (.indexOf branch-node-2-fields jf))) (mapv %)) %) ))
        join-rdd (.leftOuterJoin (JavaPairRDD/fromJavaRDD key-rdd-1) (JavaPairRDD/fromJavaRDD key-rdd-2))
        acc-rdd-1 (keg/rdd join-rdd (map (fn [[_ tuple-2]]
                                           (let [[fs-1 fs-2] [(._1 tuple-2) (-> tuple-2 ._2 .orNull)]]
                                             (concat fs-1 (or fs-2 (repeat (count branch-node-2-fields) nil)))))))]
    [node-1-name (assoc node-1-val :RESULT acc-rdd-1 :RESULT-COLUMNS (concat rdd-1-columns rdd-2-columns))] ))

(defn- inner-trgx-join [tab-trgx]
  #_(prn {:tab-trgx tab-trgx})
  (->> tab-trgx
       ((fn [node]
          (reduce (fn [[node-name {{node-fields :fields} :DATA node-result :RESULT} :as node] [branch-name branch-nodes]]
                    (when-first [[branch-name {{node-fields :fields} :DATA} :as branch-node] branch-nodes]
                      (rdd-join node branch-name (inner-trgx-join branch-node) )
                      ))
                  (init-rdd node) (-> node second :BRANCHS))))))

(defn trgx-join [tab-trgx] (-> tab-trgx inner-trgx-join second :RESULT))

(comment
  (def order-node [:dp_bolome_order
                   {:DATA {:fields [:order-dw-dt :order-dw-ts :order-dw-src-id
                                    :product-dw-id :product-dw-src-id
                                    :show-dw-id :show-dw-src-id
                                    :preview-show-dw-id :preview-show-dw-src-id
                                    :replay-show-dw-id :replay-show-dw-src-id
                                    :pay-dt :user-id :order-id
                                    :order-item-quantity :order-item-price :order-item-warehouse-id :coupon-id :event-dw-src-id
                                    :coupon-discount-amount :order-item-system-discount-amount :order-item-tax-amount :order-item-logistics-amount]}}])
  (def product-node [:d_bolome_product_category
                     {:DATA {:fields [:dw-id :dw-src-id
                                      :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                      :product-category-1-dw-id :product-category-1-dw-src-id :product-category-2-dw-id :product-category-2-dw-src-id
                                      :barcode :product-name]}}])

  (def event-node [:d_bolome_event
                   {:DATA {:fields [:dw-dt :dw-ts :dw-src-id
                                    :event-id :type-name :event-name :create-dt]
                                                        :partitions [:p_dw_dt]}}])
  (into [] (->  (init-rdd event-node) second :RESULT))
  (into []
        (keg/rdd
         (-> (rdd-join (init-rdd order-node)
                       :product
                       (init-rdd product-node))
             second :RESULT)))
  (into []
        (keg/rdd (.wholeTextFiles keg/*sc* "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_event/*")
                 (map second)
                 (mapcat #(clojure.string/split % #"\n"))))
  (def data (into [] (trgx-join (first  tab-trgx)))) 
  (count (first  data))
  )

(defn -main []
  (apply + (range 100))
  )
(comment
  (keg/connect! "local")
  (doto  (-> keg/*sc* .sc .conf)
    (.set "spark.app.name" "d_bolome_user_order")
    (.set "spark.master" "yarn")
    )
  (.close keg/*sc*)
    (def ss (->> keg/*sc* .sc (new SparkSession)) ))
