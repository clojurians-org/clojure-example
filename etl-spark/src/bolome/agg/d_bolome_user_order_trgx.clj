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

(ns bolome.agg.d_bolome_user_order_trgx
  (:require [powderkeg.core :as keg]
            [net.cgrand.xforms :as x]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]])
  (:import [org.apache.spark.sql SparkSession]
           [org.apache.spark.sql.types StringType StructField StructType]
           [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Row SaveMode RowFactory]))

(defn latest-schema []
  {:DATA {:user-id ["user-id" :STRING]}
   :CHILDREN {["dw-dt" :STRING]
              {:DATA {:dw-dt ["dw-dt" :STRING]}
               :CHILDREN {["product-dw-id" :STRING]
                          {:DATA {:product-dw-id ["product-dw-id" :INT]
                                  :product-dw-src-id ["product-dw-src-id" :STRING]
                                  :product-category-1-dw-id ["product-category-1-dw-id" :INT]
                                  :product-category-1-dw-src-id ["product-category-1-dw-src-id" :STRING]
                                  :product-category-2-dw-id ["product-category-2-dw-id" :INT]
                                  :product-category-2-dw-src-id ["product-category-2-dw-src-id" :STRING]
                                  :product-product-name ["product-product-name" :STRING]}
                           :CHILDREN {["order-dw-src-id" :STRING]
                                      {:DATA {:order-dw-src-id ["order-dw-src-id" :STRING]
                                              :order-dw-dt ["order-dw-dt" :STRING]}
                                       :CHILDREN {["order-item-id" :STRING]
                                                  {:DATA {:order-item-id ["order-item-id" :STRING]
                                                          :order-item-quantity ["order-item-quantity" :INT]
                                                          :order-item-price ["order-item-price" :DOUBLE]
                                                          :order-item-warehouse-id ["order-item-warehouse-id" :STRING]
                                                          :order-item-system-discount-amount ["order-item-system-discount-amount" :DOUBLE]
                                                          :order-item-tax-amount ["order-item-tax-amount" :DOUBLE]
                                                          :order-item-logistics-amount ["order-item-logistics-amount" :DOUBLE]}
                                                   :BRANCH {:coupon {["coupon-id" :STRING]
                                                                     {:DATA {:coupon-id ["coupon-id" :STRING]
                                                                             :coupon-discount-amount ["coupon-discount-amount" :DOUBLE]}
                                                                      :CHILDREN {}}}
                                                            :event {["event-dw-src-id" :STRING]
                                                                    {:DATA {:event-dw-src-id ["event-dw-src-id" :STRING]
                                                                            :event-type-name ["event-type-name" :STRING]
                                                                            :event-event-name ["event-event-name" :STRING]}
                                                                     :CHILDREN {}}}
                                                            :show {["show-dw-id" :STRING]
                                                                   {:DATA {:show-dw-id ["show-dw-id" :INT]
                                                                           :show-dw-src-id ["show-dw-src-id" :STRING]
                                                                           :show-show-name ["show-show-name" :STRING]
                                                                           :show-begin-ts ["show-begin-ts" :STRING]
                                                                           :show-end-ts ["show-end-ts" :STRING]}
                                                                    :CHILDREN {}}}
                                                            :preview-show {["preview-show-dw-id" :INT]
                                                                           {:DATA {:preview-show-dw-id ["preview-show-dw-id" :INT]
                                                                                   :preview-show-dw-src-id ["preview-show-dw-src-id" :STRING]
                                                                                   :preview-show-show-name ["preview-show-show-name" :STRING]
                                                                                   :preview-show-begin-ts ["preview-show-begin-ts" :STRING]
                                                                                   :preview-show-end-ts ["preview-show-end-ts" :STRING]}
                                                                            :CHILDREN {}}}
                                                            :replay-show {["replay-show-dw-id" :INT]
                                                                          {:DATA {:replay-show-dw-id ["replay-show-dw-id" :INT]
                                                                                  :replay-show-dw-src-id ["replay-show-dw-src-id" :STRING]
                                                                                  :replay-show-show-name ["replay-show-show-name" :STRING]
                                                                                  :replay-show-begin-ts ["replay-show-begin-ts" :STRING]
                                                                                  :replay-show-end-ts ["replay-show-end-ts" :STRING]}
                                                                           :CHILDREN {}}}}}}}}}}}}})

(defn deep-merge [& vals]  (if (every? map? vals)  (apply merge-with deep-merge vals)  (last vals)))
(defn realize-trgx [schema tkvs]
  (->> tkvs
       clojure.edn/read-string
       (map #(clojure.walk/prewalk (fn [node]
                                     (if (and (vector? node) (#{:INT :STRING :DOUBLE} (second node)) ) 
                                       (let [[field xtype] node
                                             field-val (-> field keyword %)]
                                         (cond
                                           (nil? field-val) nil
                                           (#{"NULL" "null"} field-val) nil
                                           (= xtype :INT) (Integer/parseInt field-val)
                                           (= xtype :DOUBLE) (Double/parseDouble field-val)
                                           :else field-val))
                                       node))  schema))
       (apply deep-merge))  )



(defn sort? [& coll] (= (sort coll) coll))
(defn tree-map [edn filters xfn]
  (if-let [origin-cur-filter (first filters)]
    (let [cur-filter (if (sequential? origin-cur-filter) origin-cur-filter (vector origin-cur-filter))
          edn-filter (if (= cur-filter ["*"]) edn
                         (let [[type & coll] cur-filter]
                           (condp = type
                             :range (->> edn (filter #(sort? (first coll) (first %) (second coll))) (into {}))
                             (select-keys edn cur-filter))))]
      (->> edn-filter
           (keep (fn [[k v]] (let [v-kv (tree-map v (rest filters) xfn)] (if (seq v-kv) [k v-kv]))))
           (into {})))
    (xfn edn)))

(defn derive-exprs [exprs trgx]
  (-> trgx
      (tree-map [:CHILDREN "*" :CHILDREN "*" :CHILDREN "*" :CHILDREN "*"]
                #(reduce (fn [node [var [xfn & params]]]
                           (assoc-in node [:DATA var] (apply xfn node params)))
                         % exprs))) )

(defn index-boolean [b] (cond (and (sequential? b) (empty? b)) 0 b 1 :else 0))
(defn latest-exprs []
  (array-map :order-item-revenue [(fn [node & params] (apply * (map (:DATA node) params))) :order-item-quantity :order-item-price]
             :order-item-base-revenue [(fn [node & params] (apply + (map (:DATA node) params))) :order-item-revenue :order-item-tax-amount :order-item-logistics-amount]
             :order-item-discount-amount [(fn [node & params] (apply + (map (:DATA node) params))) :order-item-system-discount-amount :order-item-logistics-amount]
             :order-item-coupon-cnt  [(fn [node] (->> node :BRANCH :coupon keys (keep identity) index-boolean))]
             :order-item-event-ste-cnt [(fn [node] (->> node :BRANCH :event vals first :DATA :event-type-name (= "专题") index-boolean))]
             :order-item-event-pe-cnt [(fn [node] (->> node :BRANCH :event vals first :DATA :event-type-name (= "活动") index-boolean))]
             ))

(defn -main []
  (as-> (keg/rdd (.textFile keg/*sc* "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order")
                 #_(.textFile keg/*sc* "/home/spiderdt/work/git/larluo/user/hive/warehouse/agg.db/d_bolome_user_order_test")
                 (map #(clojure.string/split % #"\001" 2))
                 (map (fn [[user-id user-tkvs]]
                        (->> [user-id
                              (->> user-tkvs
                                   (realize-trgx (latest-schema))
                                   (derive-exprs (latest-exprs))
                                   pr-str)]
                             into-array
                             RowFactory/create))) )
      $
    (.createDataFrame (->> keg/*sc* .sc (new SparkSession)) $
                      (DataTypes/createStructType (map #(DataTypes/createStructField % DataTypes/StringType false) ["user-id" "user-trgx"])))
    (.write $)
    (.format $ "parquet")
    (.mode $ SaveMode/Overwrite)
    (.save $ "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order_trgx"))

  (System/exit 0)
  )


(comment
  (keg/connect! "local")
  (def user-tkvs
    (-> (into [] (keg/rdd (.textFile keg/*sc* "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order")
                          (take 1)
                          (map #(second  (clojure.string/split % #"\001" 2)))) )
        first))

  (as-> user-tkvs $
    (realize-trgx (latest-schema) $)
    #_(tree-map $ [:CHILDREN "*" :CHILDREN "*" :CHILDREN "*" :CHILDREN "*"] (fn [x] {:here (->> x :BRANCH :coupon keys (keep identity) index-boolean)}))
    (derive-exprs (latest-exprs) $))
  )
