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

(ns hadoop.bolome.ods.d_bolome_product_category
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
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

(defn latest-ts [] (tf/unparse (->  (tf/formatter "yyyy-MM-dd'T'HH:mm:ssZ") (.withZone (t/default-time-zone)))  (t/now)))
(defn or-t [& tuple] (->> tuple (partition 2) (mapv (partial some identity))))
(defn collect-dw-id-row [max-dw-id]
  (aggregatefn
   ([] [])
   ([acc & tuple] (conj acc (vec tuple)))
   ([x] (vector {:rows (->> x
                            (reduce (fn [[idx acc-t] [dw-id & t]]
                                      [(cond-> idx (nil? dw-id) inc) (conj acc-t (vec (cons (or dw-id (inc idx)) t)))])
                                    [max-dw-id []])
                            second)}))))

(defmapcatfn split-rows [x] (if (:rows x) (:rows x) x))

(defn -main []
  (as-> (<- [?dw-src-id ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?barcode ?product-name ?category-1 ?category-2]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_product_category" :skip-header? true :delimiter ",")
             :> ?barcode ?product-name ?category-1 ?category-2)
            (identity ?barcode :> ?dw-src-id)
            (identity (latest-ts) :> ?dw-first-ts)
            (subs ?dw-first-ts 0 10 :> ?dw-first-dt )
            (identity (latest-ts) :> ?dw-latest-ts)
            (subs ?dw-latest-ts 0 10 :> ?dw-latest-dt))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/dd_bolome_product_category"
                       :outfields ["?dw-src-id" "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?barcode" "?product-name" "?category-1" "?category-2"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))

  (defn latest-max-dw-id [] (-> (??<- [?max-dw-id]
                                      ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_product_category" :delimiter "\001")
                                       :#> 9 {0 ?dw-id})
                                      (c/max ?dw-id :> ?max-dw-id))
                                ffirst (or 0)))
  (as-> (<- [?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?barcode ?product-name ?category-1 ?category-2]
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/dd_bolome_product_category" :delimiter "\001")
               :> ?j-dw-src-id
               !!src-dw-first-dt !!src-dw-first-ts !!src-dw-latest-dt !!src-dw-latest-ts
               !!src-barcode !!src-product-name !!src-category-1 !!src-category-2)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_product_category" :delimiter "\001")
               :> !!tgt-dw-id ?j-dw-src-id
               !!tgt-dw-first-dt !!tgt-dw-first-ts !!tgt-dw-latest-dt !!tgt-dw-latest-ts
               !!tgt-barcode !!tgt-product-name !!tgt-category-1 !!tgt-category-2)
              (or-t nil !!tgt-dw-id
                    !!tgt-dw-first-dt !!src-dw-first-dt !!tgt-dw-first-ts !!src-dw-first-ts
                    !!tgt-dw-latest-dt !!src-dw-latest-dt !!tgt-dw-latest-ts !!src-dw-latest-ts
                    !!src-barcode !!tgt-barcode !!src-product-name !!tgt-product-name !!src-category-1 !!tgt-category-1 !!src-category-2 !!tgt-category-2
                    :> !mg-dw-id
                    ?mg-dw-first-dt ?mg-dw-first-ts ?mg-dw-latest-dt ?mg-dw-latest-ts
                    ?mg-barcode ?mg-product-name ?mg-category-1 ?mg-category-2)
              ((collect-dw-id-row (latest-max-dw-id)) !mg-dw-id ?j-dw-src-id
                 ?mg-dw-first-dt ?mg-dw-first-ts ?mg-dw-latest-dt ?mg-dw-latest-ts
                 ?mg-barcode ?mg-product-name ?mg-category-1 ?mg-category-2
                 :> ?dw-id-rows)
              (split-rows ?dw-id-rows
                            :> ?dw-id ?dw-src-id
                            ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
                            ?barcode ?product-name ?category-1 ?category-2))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/ds_bolome_product_category"
                       :outfields ["?dw-id" "?dw-src-id" "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?barcode" "?product-name" "?category-1" "?category-2"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))

  (as-> (<- [?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?barcode ?product-name ?category-1 ?category-2]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/ds_bolome_product_category" :delimiter "\001")
             :> ?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?barcode ?product-name ?category-1 ?category-2))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_product_category"
                       :outfields ["?dw-id" "?dw-src-id"
                                   "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?barcode" "?product-name" "?category-1" "?category-2"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))
  )
