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

(ns hadoop.bolome.model.dw_keyword.d_bolome_product_category
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

(defmapcatfn unpivot [& ks] (-> ks vec))
(defmapcatfn unpivot-single [ks] (-> (or (:rows ks) ks) vec))

(defn collect-dw-id-row [max-dw-id]
  (aggregatefn
   ([] [])
   ([acc & tuple] (conj acc (vec tuple)))
   ([x] (vector {:rows (->> x
                            (reduce (fn [[idx acc-t] [dw-id & t]]
                                      [(cond-> idx (nil? dw-id) inc) (conj acc-t (vec (cons (or dw-id (inc idx)) t)))])
                                    [max-dw-id []])
                            second)}))))

(defaggregatefn collect-set
  ([] #{}) 
  ([acc & coll] (clojure.set/union acc (-> coll set)))
  ([x] x))

(defn -main []
  (defn latest-max-dw-id [] (-> (??<- [?max-dw-id]
                                      ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dw_keyword" :delimiter "\001")
                                       :#> 9 {0 ?dw-id})
                                      (c/max ?dw-id :> ?max-dw-id))
                                ffirst (or 0)))
  
  (as-> (<- [?dw-keyword]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_product_category" :delimiter "\001")
             :> ?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?barcode ?product-name ?category-1 ?category-2)
            (unpivot ?category-1 ?category-2 :> ?dw-keyword)
            (:distinct true))
      $
      (<- [?dw-id ?dw-keyword]
          ($ :> ?j-dw-keyword)
          ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dw_keyword" :delimiter "\001")
           :> !!tgt-dw-id ?j-dw-keyword)
          ((collect-dw-id-row (latest-max-dw-id)) !!tgt-dw-id ?j-dw-keyword :> ?dw-id-rows)
          (unpivot-single ?dw-id-rows :> ?dw-id ?dw-keyword)
          )
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dw_s_keyword"
                         :outfields ["?dw-id" "?dw-keyword"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :compression  :enable) $))

  (as-> (<- [?dw-id ?dw-keyword]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dw_s_keyword" :delimiter "\001")
             :> ?dw-id ?dw-keyword))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dw_keyword"
                       :outfields ["?dw-id" "?dw-keyword"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))
  
  
  )
