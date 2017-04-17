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

(defn latest-tab-trgx []
  {:dp_bolome_order
   {:DATA {:fields [:dw-dt :dw-ts :dw-src-id
                    :product-dw-id :product-dw-src-id
                    :show-dw-id :show-dw-src-id
                    :preview-show-dw-id :preview-show-dw-src-id
                    :replay-show-dw-id :replay-show-dw-src-id
                    :pay-dt :user-id :order-id
                    :order-item-quantity :order-item-price :order-item-warehouse-id :coupon-id :event-dw-src-id
                    :coupon-discount-amount :order-item-system-discount-amount :order-item-tax-amount :order-item-logistics-amount]
           :repartition 16}
    :BRANCHS {:product {:d_bolome_product_category
                         {:DATA {:fields [:dw-id :dw-src-id
                                          :dw-first-dt :dw-first-ts :dw-latest-dt :dw-latest-ts
                                          :category-1-dw-id :category-1-dw-src-id :category-2-dw-id :category-2-dw-src-id
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
                             :BRANCHS {}}}}}})

(defn latest-schema []
  {["user-id" :STRING]
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
                               :CHILDREN {["dw-src-id" :STRING]
                                          {:DATA {:order-dw-src-id ["dw-src-id" :STRING]
                                                  :order-dw-dt ["dw-dt" :STRING]}
                                           :CHILDREN {["product-dw-id" :STRING]
                                                      {:DATA {:order-item-id ["product-dw-id" :STRING]
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
                                                                               :CHILDREN {}}}}}}}}}}}}}})

(defn index-boolean [b] (cond (and (sequential? b) (empty? b)) 0 b 1 :else 0))
(defn latest-exprs []
  (array-map :order-item-revenue [(fn [node & params] (apply * (map (:DATA node) params))) :order-item-quantity :order-item-price]
             :order-item-base-revenue [(fn [node & params] (apply + (map (:DATA node) params))) :order-item-revenue :order-item-tax-amount :order-item-logistics-amount]
             :order-item-discount-amount [(fn [node & params] (apply + (map (:DATA node) params))) :order-item-system-discount-amount :order-item-logistics-amount]
             :order-item-coupon-cnt  [(fn [node] (->> node :BRANCH :coupon keys (keep identity) index-boolean))]
             :order-item-event-ste-cnt [(fn [node] (->> node :BRANCH :event vals first :DATA :event-type-name (= "专题") index-boolean))]
             :order-item-event-pe-cnt [(fn [node] (->> node :BRANCH :event vals first :DATA :event-type-name (= "活动") index-boolean))]
             ))

(defn init-rdd [[node-name {{repartition :repartition partitions :partitions fields :fields} :DATA :as node-val}]]
  (let [hive-path "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db"]
    [node-name
     (-> (assoc node-val
                :RESULT
                (apply keg/rdd (if partitions
                                 (keg/rdd (.wholeTextFiles keg/*sc* (->> "*" (repeat (count partitions)) (concat [hive-path (name node-name)]) (clojure.string/join "/")))
                                          (map second)
                                          (mapcat #(clojure.string/split % #"\n")))
                                 (.textFile keg/*sc* (str hive-path "/" (name node-name))))
                       (map #(clojure.string/split % #"\001"))
                       (when repartition [:partitions repartition]))))]))

(defn rdd-join [rdd-1 rdd-2 rdd-fs-cnt]
  #_(-> (.leftOuterJoin (JavaPairRDD/fromJavaRDD rdd-1) (JavaPairRDD/fromJavaRDD rdd-2))
         (keg/rdd (map (fn [[_ tuple-2]]
                         (let [[fs-1 fs-2] [(._1 tuple-2) (-> tuple-2 ._2 .orNull)]]
                           (vec (concat fs-1 (or fs-2 (repeat rdd-fs-cnt nil)))))))))
  (let [rdd-2-map (into {} rdd-2)]
    (keg/rdd rdd-1
             (map (fn [[jfs fs-1]]
                    (vec (concat fs-1 (get rdd-2-map jfs (repeat rdd-fs-cnt nil)))))))))

(defn node-join [[node-1-name {{node-1-fields :fields} :DATA rdd-1 :RESULT rdd-1-columns :BRANCH-FIELDS :as node-1-val} :as node-1]
                 branch-name
                 [node-2-name {{node-2-fields :fields} :DATA rdd-2 :RESULT rdd-2-columns :BRANCH-FIELDS} :as node-2]]
  (let [prefix-branch-field (fn [branch-name field] (->> [branch-name field] (map name) (clojure.string/join "-") keyword))
        branch-node-2-fields (mapv (partial prefix-branch-field branch-name) node-2-fields)
        jfs (->> (apply clojure.set/intersection (map set [node-1-fields (remove #(= (prefix-branch-field branch-name :dw-src-id) %) branch-node-2-fields)])))
        key-rdd-1 (keg/rdd rdd-1 (map #(vector (->> jfs (mapv (fn [jf] (.indexOf node-1-fields jf))) (mapv %)) %)))
        key-rdd-2 (keg/rdd rdd-2 (map #(vector (->> jfs (mapv (fn [jf] (.indexOf branch-node-2-fields jf))) (mapv %)) %)))
        acc-rdd-1 (rdd-join key-rdd-1 key-rdd-2 (+  (count node-2-fields) (count rdd-2-columns)))]
    [node-1-name (assoc node-1-val :RESULT acc-rdd-1 :BRANCH-FIELDS (concat rdd-1-columns branch-node-2-fields rdd-2-columns))]))

(defn- inner-trgx-join [tab-trgx]
  (->> tab-trgx
       ((fn [node]
          (reduce (fn [[node-name {{node-fields :fields} :DATA node-result :RESULT} :as node] [branch-name branch-nodes]]
                    (when-first [[branch-node-name {{node-fields :fields} :DATA} :as branch-node] branch-nodes]
                      (node-join node branch-name (inner-trgx-join branch-node))))
                  (init-rdd node) (-> node second :BRANCHS))))))

(defn trgx-join [tab-trgx]
  (let [{{fields :fields} :DATA rdd :RESULT branch-fields :BRANCH-FIELDS} (->> tab-trgx inner-trgx-join second)]
    {:rdd rdd :fields (concat fields branch-fields)}))

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

(defn deep-merge [& vals]  (if (every? map? vals)  (apply merge-with deep-merge vals)  (last vals)))
(defn derive-exprs [exprs trgx]
  (->> (tree-map trgx [:CHILDREN "*" :CHILDREN "*" :CHILDREN "*" :CHILDREN "*"]
                 #(reduce (fn [node [var [xfn & params]]]
                            (assoc-in node [:DATA var] (apply xfn node params)))
                          % exprs)) 
      (deep-merge trgx)) )

(defn collect-trgx [fields schema]
  (fn ([] {})
    ([acc x] (if (map? x) (deep-merge acc x)
                 (deep-merge acc (as-> (zipmap (reverse fields) (reverse x))  $
                                   (clojure.walk/prewalk
                                    (fn [node]
                                      (if (and (vector? node) (#{:INT :STRING :DOUBLE} (second node)) ) 
                                        (let [[field xtype] node
                                              field-val (-> field keyword $)]
                                          (cond
                                            (nil? field-val) nil
                                            (#{"NULL" "null" ""} field-val) nil
                                            (= xtype :INT) (Integer/parseInt field-val)
                                            (= xtype :DOUBLE) (Double/parseDouble field-val)
                                            :else field-val))
                                        node))
                                    schema)) )))
    ([x] x)))

(comment
  (deep-merge {} {:a 3} {:b 4} {:a {:d 5}})p
  (sequence (x/reduce (collect-trgx [:a :b :c :d] {["a" :STRING] {:DATA {:b {["b" :STRING]  ["c" :STRING]}} :CHILDREN {}}})) [[1 2 3 4] [1 3 4 5] [11 22 33 44]])
  )
(defn -main []
  (let [{:keys [rdd fields]} (trgx-join (first (latest-tab-trgx)))]
    (as-> (keg/rdd rdd (map #(vector (nth % (.indexOf fields :user-id)) %))) $
      (keg/by-key $ (x/reduce (collect-trgx fields (latest-schema))))
      (keg/rdd $ (map (fn [[user-id user-id-trgx]]
                        (->> [user-id (derive-exprs (latest-exprs) (->  user-id-trgx first second))]
                             pr-str  vector into-array RowFactory/create
                             ))))
      (.createDataFrame (->> keg/*sc* .sc (new SparkSession)) $
                        (DataTypes/createStructType (map #(DataTypes/createStructField % DataTypes/StringType false) ["user-id-trgx"])))
      (.write $)
      (.format $ "parquet")
      (.mode $ SaveMode/Overwrite)
      (.save $ "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order_trgx")))

  (System/exit 0)
  )

(comment
  (keg/connect! "local")
  (def ss (->> keg/*sc* .sc (new SparkSession)))
  (doto  (-> keg/*sc* .sc .conf)
    (.set "spark.app.name" "d_bolome_user_order")
    (.set "spark.master" "yarn"))
  (.close keg/*sc*)
  (def result *1)

  )
