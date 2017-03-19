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

(ns hadoop.bolome.mlin.d_bolome_user_order
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn select-fields with-job-conf]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile hfs-textline]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]])
  (:gen-class))

(set-level! :warn)

(defn parse-dt [dt] (tf/parse (tf/formatter "yyyy-MM-dd") dt))
(defn unparse-dt [dt-obj] (tf/unparse (tf/formatter "yyyy-MM-dd") dt-obj))
(defn offset-dt [dt n] (-> dt parse-dt  (t/plus (t/days n)) unparse-dt))
(defn dt-ge-get [dt-1 dt-2] (if (<= (compare dt-2 dt-1) 0) dt-1))
(defn str-min [& coll] (-> coll sort first))

(defn dt-rebase [[new-start-dt new-end-dt] [start-dt end-dt]]
  (let [dt-interval (-> (t/interval (parse-dt start-dt) (parse-dt end-dt)) t/in-days)]
    [(or new-start-dt (offset-dt new-end-dt (- dt-interval)))
     (or new-end-dt (offset-dt new-start-dt dt-interval))]))

(defn build-kind-shift [[dw-min-dt dw-max-dt] [dm-start-dt dm-end-dt] n back-step intervals]
  (let [his-max-dt (str-min dw-max-dt (offset-dt dm-start-dt -1))
        train-base-seq (->> (periodic-seq (parse-dt his-max-dt) (t/days (- back-step)))
                            (map unparse-dt) (take-while #(dt-ge-get % dw-min-dt)))
        cal-dts   (fn [dt] (let [[y-start-dt y-end-dt] (dt-rebase [nil dt] [dm-start-dt dm-end-dt])
                                 [x-max-dt _] (dt-rebase [nil y-end-dt] [his-max-dt dm-end-dt])
                                 x-dts (->> intervals sort
                                            (map #(dt-ge-get (offset-dt x-max-dt (- %)) dw-min-dt))
                                            (take-while identity)
                                            (#(map vector % (repeat x-max-dt)))
                                            (zipmap (sort intervals)))]
                             (when (seq x-dts) {:y-cut [y-start-dt y-end-dt] :xs-cut x-dts})))
        train-seq (->> train-base-seq (map cal-dts) (take-while identity))
        test-seq [(cal-dts dm-end-dt)]
        xs-count (count intervals)
        train-times-seq (if (< n (count train-seq))
                          (take n train-seq)
                          (take-while #(= (-> % :xs-cut count) xs-count) train-seq))]
    {:train-shift-cuts (vec train-times-seq) :test-shift-cuts (vec test-seq)}))

(defn sort? [& coll] (= (sort coll) coll))
(defn tree-nodes [edn filters]
  (if-let [origin-cur-filter (first filters)]
    (let [cur-filter (if (sequential? origin-cur-filter) origin-cur-filter (vector origin-cur-filter))
          edn-filter (if (= cur-filter ["*"]) edn
                         (let [[type & coll] cur-filter]
                           (condp = type
                             :range (->> edn (filter #(sort? (first coll) (first %) (second coll))) (into {}))
                             (select-keys edn cur-filter))))]
      (->> edn-filter (mapcat (fn [[k v]] (let [v-kv (tree-nodes v (rest filters))] v-kv))) vec))
    [edn]))

(defn index-boolean [b] (cond (and (sequential? b) (empty? b)) 0 b 1 :else 0))
(defn process-y-cut [[y-start-dt y-end-dt :as y-cut] extractor trgx]
  {:cut y-cut :label (-> trgx (tree-nodes [:CHILDREN [:range y-start-dt y-end-dt]]) index-boolean)})

(defn parse-exprs [exprs data]
  (reduce (fn [m [var [xfn & params]]]
            (assoc m var (str (apply xfn (map #(-> m % Double/parseDouble) params)))))
          data exprs))

(defn process-x-cut [[x-start-dt x-end-dt :as x-cut] {:keys [exprs order-item-fields product-ids product-item-fields product-group-var product-group-item-fields] :as extractor} trgx]
  {:cut x-cut
   :label (-> trgx (tree-nodes [:CHILDREN [:range x-start-dt x-end-dt]]) index-boolean)
   :order-cnt (-> trgx (tree-nodes [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*" :CHILDREN "*" :DATA :order-dw-src-id]) distinct count)
   :order-items (let [order-item-data (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*" :CHILDREN "*" :CHILDREN "*" :DATA])
                            order-item-data-ext (mapv (partial parse-exprs exprs) order-item-data)
                            order-item-data-numeric (mapv #(->> order-item-fields (mapv (fn [f] [f (->> % f Double/parseDouble)])) (into {})) order-item-data-ext)]
                        (if (seq order-item-data-numeric)
                          (apply merge-with + order-item-data-numeric)
                          (zipmap order-item-fields (repeat 0.0))))
   :products (->> product-ids
                 (map (fn [product-id]
                        (let [product-item-data (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt]
                                                                  :CHILDREN product-id :CHILDREN "*" :CHILDREN "*" :DATA ])
                              product-item-data-ext (mapv (partial parse-exprs exprs) product-item-data)
                              product-item-data-numeric (mapv #(->> product-item-fields (mapv (fn [f] [f (->> % f Double/parseDouble)])) (into {}))
                                                              product-item-data-ext)]
                          [product-id (if (seq product-item-data-numeric)
                                        (apply merge-with + product-item-data-numeric)
                                        (zipmap product-item-fields (repeat 0.0)))] )))
                        (into {}))
   :product-groups (->> (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*"])
                        (map (fn [product-node]
                               (let [product-group-id (first (tree-nodes product-node [:DATA product-group-var]))
                                     product-group-item-data (tree-nodes product-node [:CHILDREN "*" :CHILDREN "*" :DATA])
                                     product-group-item-data-ext (mapv (partial parse-exprs exprs) product-group-item-data)
                                     product-group-item-data-numeric (mapv #(->> product-group-item-fields (mapv (fn [f] [f (->> % f Double/parseDouble)])) (into {}))
                                                                           product-group-item-data-ext)]
                                 {product-group-id (if (seq product-group-item-data-numeric)
                                                     (apply merge-with + product-group-item-data-numeric)
                                                     (zipmap product-group-item-fields (repeat 0.0)))} )))
                              (apply merge-with (partial merge-with +)))})
 
(defmapcatfn kind-shift-cut-trgx [shifts extractor trgx]
  (let [process-shift-cut (fn [{:keys [y-cut xs-cut]} extractor trgx]
                            {:y (process-y-cut y-cut extractor trgx)
                             :xs (into {} (map (fn [[x x-cut]] [x (process-x-cut x-cut extractor trgx)]) xs-cut))} )
        process-kind-shift (fn [[kind shifts] extractor trgx]
                             (let [dm-ds-kind (condp = kind :train-shift-cuts "train" :test-shift-cuts "test")]
                               (mapv #(merge {:dm-ds-kind dm-ds-kind} (process-shift-cut % extractor trgx))  shifts)))]
    (vec (mapcat #(process-kind-shift % extractor trgx) shifts))) )

(comment
  (def user-trgx
    (-> (??<- [?user-trgx-edn]
           ((hfs-textline "/home/spiderdt/work/git/larluo/user/hive/warehouse/agg.db/d_bolome_user_order_trgx_test") :> ?row)
           (clojure.string/split ?row #"\001" 2 :> ?user-id ?user-trgx)
           (clojure.edn/read-string ?user-trgx :> ?user-trgx-edn))
        ffirst))
  (tree-nodes user-trgx [:CHILDREN [:range "1970-01-01" "9999-12-31"] :CHILDREN "*" ])

  (prn
   (kind-shift-cut-trgx (build-kind-shift ["2015-07-01" "2016-06-30"] ["2016-08-01" "2016-10-01"]  30 3 #{3 6 13 20 27 59 29})
                       {:exprs (array-map :order-item-revenue [* :order-item-quantity :order-item-price]
                                           :order-item-base-revenue [+ :order-item-revenue :order-item-tax-amount :order-item-logistics-amount]
                                           :order-item-discount-amount [+ :order-item-system-discount-amount :order-item-logistics-amount])
                        :order-item-fields [:order-item-revenue :order-item-base-revenue :order-item-discount-amount]
                        #_[:order-coupon :order-ste :order-pe :order-debut :order-replay]
                        :product-ids #{1125 1126}
                        :product-item-fields [:order-item-quantity :order-item-revenue :order-item-base-revenue :order-item-discount-amount]
                        #_[:order-coupon :order-ste :order-pe :order-debut :order-replay]
                        :product-group-var :product-category-1-dw-id
                        :product-group-item-fields [:order-item-quantity :order-item-revenue :order-item-base-revenue :order-item-discount-amount]}
                       user-trgx)
   )
  )

(defn -main []
  (with-job-conf {"mapreduce.input.fileinputformat.input.dir.recursive" "true"
                  "mapred.reduce.tasks" "1"
                  "mapred.job.map.memory.mb" "1024"
                  "mapred.job.reduce.memory.mb" "1024"
                  "mapred.compress.map.output" "true"
                  "mapred.map.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec"
                  "mapred.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec" }
    (as-> (<- [?user-id ?dm-ds-kind ?user-order-tkv]
              #_((hfs-textline "/home/spiderdt/work/git/larluo/user/hive/warehouse/agg.db/d_bolome_user_order_trgx_test") :> ?row)
              ((hfs-textline "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order_trgx") :> ?row)
              (clojure.string/split ?row #"\001" 2 :> ?user-id ?user-trgx)
              (clojure.edn/read-string ?user-trgx :> ?user-trgx-edn)
              (kind-shift-cut-trgx (build-kind-shift ["2015-07-01" "2016-06-30"] ["2016-08-01" "2016-10-01"]  30 3 #{3 6 13 20 27 59 29})
                                   {:exprs (array-map :order-item-revenue [* :order-item-quantity :order-item-price]
                                                      :order-item-base-revenue [+ :order-item-revenue :order-item-tax-amount :order-item-logistics-amount]
                                                      :order-item-discount-amount [+ :order-item-system-discount-amount :order-item-logistics-amount])
                                    :order-item-fields [:order-item-revenue :order-item-base-revenue :order-item-discount-amount]
                                    #_[:order-coupon :order-ste :order-pe :order-debut :order-replay]
                                    :product-ids #{1125 1126}
                                    :product-item-fields [:order-item-quantity :order-item-revenue :order-item-base-revenue :order-item-discount-amount]
                                    #_[:order-coupon :order-ste :order-pe :order-debut :order-replay]
                                    :product-group-var :product-category-1-dw-id
                                    :product-group-item-fields [:order-item-quantity :order-item-revenue :order-item-base-revenue :order-item-discount-amount]}
                                   ?user-trgx-edn
                                   :> ?user-order-tkv)
              (mapv ?user-order-tkv [:dm-ds-kind] :> ?dm-ds-kind))
        $
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ml_in.db/d_bolome_user_order"
                         :outfields ["?user-id" "?user-order-tkv"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :templatefields ["?dm-ds-kind"]
                         :sink-template "p_ds=%s"
                         :compression  :enable) $)))
  )

