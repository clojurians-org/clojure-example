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

(ns hadoop.bolome.model.d_bolome_dau
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


(defn -main []
  
  (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
             ?dt ?dau]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_dau" :skip-header? false :delimiter "\001")
             :> ?dw-dt ?dw-ts ?dw-src-id
                ?dt ?dau))
      $
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_dau"
                         :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                     "?dt" "?dau"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :templatefields ["?dw-dt"]
                         :sink-template "p_dw_dt=%s"
                         :compression  :enable) $))
  )
