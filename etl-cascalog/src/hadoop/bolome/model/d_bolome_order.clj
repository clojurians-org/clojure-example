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

(ns hadoop.bolome.model.d_bolome_order
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

(defn -main []
  (with-job-conf {"mapreduce.input.fileinputformat.input.dir.recursive" "true"
                "mapred.reduce.tasks" "1"
                "mapred.job.map.memory.mb" "1024"
                "mapred.job.reduce.memory.mb" "1024"
                "mapred.output.compression.type" "BLOCK"}
    (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
             ?product-dw-id ?j-product-dw-src-id
             !!show-dw-id ?j-show-dw-src-id
             !!preview-show-dw-id ?j-preview-show-dw-src-id
             !!replay-show-dw-id ?j-replay-show-dw-src-id
             ?pay-dt ?user-id ?order-id
             ?quantity ?price ?warehouse-id ?coupon-id ?event-id
             ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_order" :delimiter "\001")
             :> ?dw-dt ?dw-ts ?dw-src-id
                ?pay-dt ?user-id ?order-id ?j-product-dw-src-id
                ?quantity ?price ?warehouse-id ?j-show-dw-src-id ?j-preview-show-dw-src-id ?j-replay-show-dw-src-id ?coupon-id ?event-id
             ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount)
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_product_category" :delimiter "\001")
             :> ?product-dw-id ?j-product-dw-src-id _ _ _ _  _ _ _ _  _ _)
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
             :> !!show-dw-id ?j-show-dw-src-id  _ _ _ _    _ _ _ _)
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
             :> !!preview-show-dw-id ?j-preview-show-dw-src-id  _ _ _ _    _ _ _ _)
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
             :> !!replay-show-dw-id ?j-replay-show-dw-src-id  _ _ _ _    _ _ _ _))
      $
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dp_bolome_order"
                         :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                     "?product-dw-id" "?j-product-dw-src-id"
                                     "!!show-dw-id" "?j-show-dw-src-id"
                                     "!!preview-show-dw-id" "?j-preview-show-dw-src-id"
                                     "!!replay-show-dw-id" "?j-replay-show-dw-src-id"
                                     "?pay-dt" "?user-id" "?order-id"
                                     "?quantity" "?price" "?warehouse-id" "?coupon-id" "?event-id"
                                     "?copon-discount-amount" "?system-discount-amount" "?tax-amount" "?logistics-amount"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :compression  :enable
                         ) $))    )
  (with-job-conf {"mapred.child.java.opts" "-Xmx4096m"
                  "mapreduce.input.fileinputformat.input.dir.recursive" "true"
                  "mapred.reduce.tasks" "1"
                  "mapred.job.map.memory.mb" "8192"
                  "mapred.job.reduce.memory.mb" "1024"}
    (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
               ?product-dw-id ?j-product-dw-src-id
               !show-dw-id ?j-show-dw-src-id
               !preview-show-dw-id ?j-preview-show-dw-src-id
               !replay-show-dw-id ?j-replay-show-dw-src-id
               ?pay-dt ?user-id ?order-id
               ?quantity ?price ?warehouse-id ?coupon-id ?event-id
               ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount]
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/df_bolome_order" :delimiter "\001")
               :> ?dw-dt ?dw-ts ?dw-src-id
                  ?product-dw-id ?j-product-dw-src-id
                  !show-dw-id ?j-show-dw-src-id
                  !preview-show-dw-id ?j-preview-show-dw-src-id
                  !replay-show-dw-id ?j-replay-show-dw-src-id
                  ?pay-dt ?user-id ?order-id
                  ?quantity ?price ?warehouse-id ?coupon-id ?event-id
                  ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount))
        $
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_order"
                         :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                     "?product-dw-id" "?j-product-dw-src-id"
                                     "!show-dw-id" "?j-show-dw-src-id"
                                     "!preview-show-dw-id" "?j-preview-show-dw-src-id"
                                     "!replay-show-dw-id" "?j-replay-show-dw-src-id"
                                     "?pay-dt" "?user-id" "?order-id"
                                     "?quantity" "?price" "?warehouse-id" "?coupon-id" "?event-id"
                                     "?copon-discount-amount" "?system-discount-amount" "?tax-amount" "?logistics-amount"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :templatefields ["?dw-dt"]
                         :sink-template "p_dw_dt=%s"
                         :compression  :enable) $))
    )
  )
