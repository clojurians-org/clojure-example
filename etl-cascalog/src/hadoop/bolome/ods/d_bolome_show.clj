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

(ns hadoop.bolome.ods.d_bolome_show
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile hfs-textline]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]
            [taoensso.timbre :refer [info debug warn set-level!]])
  (:gen-class))


(set-level! :warn)

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
             ?show-id ?show-name ?begin-ts ?end-ts]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_show" :skip-header? true :delimiter ",")
             :> ?show-id ?show-name ?begin-ts ?end-ts)
            (identity ?show-id :> ?dw-src-id)
            (identity ?begin-ts :> ?dw-first-ts)
            (subs ?dw-first-ts 0 10 :> ?dw-first-dt )
            (identity ?begin-ts :> ?dw-latest-ts)
            (subs ?dw-latest-ts 0 10 :> ?dw-latest-dt))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/dd_bolome_show"
                       :outfields ["?dw-src-id" "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?show-id" "?show-name" "?begin-ts" "?end-ts"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))

  (defn latest-max-dw-id [] (-> (??<- [?max-dw-id]
                                      ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_show" :delimiter "\001")
                                       :#> 9 {0 ?dw-id})
                                      (c/max ?dw-id :> ?max-dw-id))
                                ffirst (or 0)))
  
  (as-> (<- [?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?show-id ?show-name ?begin-ts ?end-ts]
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/dd_bolome_show" :delimiter "\001")
               :> ?j-dw-src-id
               !!src-dw-first-dt !!src-dw-first-ts !!src-dw-latest-dt !!src-dw-latest-ts
               !!src-show-id !!src-show-name !!src-begin-ts !!src-end-ts)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_show" :delimiter "\001")
               :> !!tgt-dw-id ?j-dw-src-id
               !!tgt-dw-first-dt !!tgt-dw-first-ts !!tgt-dw-latest-dt !!tgt-dw-latest-ts
               !!tgt-show-id !!tgt-show-name !!tgt-begin-ts !!tgt-end-ts)
              (or-t nil !!tgt-dw-id
                    !!tgt-dw-first-dt !!src-dw-first-dt !!tgt-dw-first-ts !!src-dw-first-ts
                    !!tgt-dw-latest-dt !!src-dw-latest-dt !!tgt-dw-latest-ts !!src-dw-latest-ts
                    !!src-show-id !!tgt-show-id !!src-show-name !!tgt-show-name !!src-begin-ts !!tgt-begin-ts !!src-end-ts !!tgt-end-ts
                    :> !mg-dw-id
                    ?mg-dw-first-dt ?mg-dw-first-ts ?mg-dw-latest-dt ?mg-dw-latest-ts
                    ?mg-show-id ?mg-show-name ?mg-begin-ts ?mg-end-ts)
              ((collect-dw-id-row (latest-max-dw-id)) !mg-dw-id ?j-dw-src-id
                 ?mg-dw-first-dt ?mg-dw-first-ts ?mg-dw-latest-dt ?mg-dw-latest-ts
                 ?mg-show-id ?mg-show-name ?mg-begin-ts ?mg-end-ts
                 :> ?dw-id-rows)
              (split-rows ?dw-id-rows
                            :> ?dw-id ?dw-src-id
                            ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
                            ?show-id ?show-name ?begin-ts ?end-ts))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/ds_bolome_show"
                       :outfields ["?dw-id" "?dw-src-id" "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?show-id" "?show-name" "?begin-ts" "?end-ts"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))

  (as-> (<- [?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?show-id ?show-name ?begin-ts ?end-ts]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/ds_bolome_show" :delimiter "\001")
             :> ?dw-id ?dw-src-id
             ?dw-first-dt ?dw-first-ts ?dw-latest-dt ?dw-latest-ts
             ?show-id ?show-name ?begin-ts ?end-ts))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_show"
                       :outfields ["?dw-id" "?dw-src-id"
                                   "?dw-first-dt" "?dw-first-ts" "?dw-latest-dt" "?dw-latest-ts"
                                   "?show-id" "?show-name" "?begin-ts" "?end-ts"]
                       :delimiter "\001"
                       :quote ""
                       :sinkmode :replace
                       :compression  :enable) $))
  )
