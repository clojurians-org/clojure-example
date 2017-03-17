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

(ns hadoop.bolome.agg.d_bolome_user_order
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

(defmapcatfn unpivot [& ks] (-> ks vec))
(defmapcatfn unpivot-single [ks] (-> (or (:rows ks) ks) vec))
(defaggregatefn pivot-single ([] []) ([acc ks] (conj acc ks)) ([x] [[x]]))

(comment
  (def data [["a" [11 "aa"]]
             ["a" 22]
             ["b" 33]
             ["b" 44]
             ["b" 55]])
  (??<- [?a ?x]
        (data :> ?a ?b)
        (pivot-single ?b :> ?x))  )

(defn -main []
  (with-job-conf {"mapreduce.input.fileinputformat.input.dir.recursive" "true"
                  "mapred.reduce.tasks" "1"
                  "mapred.job.map.memory.mb" "1024"
                  "mapred.job.reduce.memory.mb" "1024"
                  "mapred.compress.map.output" "true"
                  ; "mapred.map.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec"
                  "mapred.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec"}
    (as-> (<- [?user-id ?user-tkvs]
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/dp_bolome_order" :delimiter "\001")
               :> ?order-dw-dt ?order-dw-ts ?order-dw-src-id
                  !j-product-dw-id !product-dw-src-id
                  !j-show-dw-id !show-dw-src-id
                  !j-preview-show-dw-id !preview-show-dw-src-id
                  !j-replay-show-dw-id !replay-show-dw-src-id
                  ?pay-dt ?user-id ?order-id
                  ?order-item-quantity ?order-item-price ?order-item-warehouse-id ?coupon-id !j-event-dw-src-id
                  ?coupon-discount-amount ?order-item-system-discount-amount ?order-item-tax-amount ?order-item-logistics-amount)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_product_category" :delimiter "\001")
               :> !j-product-dw-id !!_product-dw-src-id
                  !!_product-dw-first-dt !!_product-dw-first-ts !!_product-dw-latest-dt !!_product-dw-latest-ts
                  !!product-category-1-dw-id !!product-category-1-dw-src-id !!product-category-2-dw-id !!product-category-2-dw-src-id
                  !!_product-barcode !!product-product-name)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_event" :delimiter "\001")
               :> !!_event-dw-dt !!_event-dw-ts !j-event-dw-src-id
                  !!event-event-id !!event-type-name !!event-event-name !!event-create-dt)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
               :> !j-show-dw-id !!_show-dw-src-id
                  !!_show-dw-first-dt !!_show-dw-first-ts !!_show-dw-latest-dt !!_show-dw-latest-ts
                  !!show-show-id !!show-show-name !!show-begin-ts !!show-end-ts)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
               :> !j-preview-show-dw-id !!_preview-show-dw-src-id
               !!_preview-show-dw-first-dt !!_preview-show-dw-first-ts !!_preview-show-dw-latest-dt !!_preview-show-dw-latest-ts
               !!preview-show-show-id !!preview-show-show-name !!preview-show-begin-ts !!preview-show-end-ts)
              ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/model.db/d_bolome_show" :delimiter "\001")
               :> !j-replay-show-dw-id !!_replay-show-dw-src-id
               !!_replay-show-dw-first-dt !!_replay-show-dw-first-ts !!_replay-show-dw-latest-dt !!_replay-show-dw-latest-ts
               !!replay-show-show-id !!replay-show-show-name !!replay-show-begin-ts !!replay-show-end-ts)
              (hash-map :user-id ?user-id :dw-dt ?order-dw-dt
                        :product-dw-id !j-product-dw-id :product-dw-src-id !product-dw-src-id
                        :product-category-1-dw-id !!product-category-1-dw-id :product-category-1-dw-src-id !!product-category-1-dw-src-id
                        :product-category-2-dw-id !!product-category-2-dw-id :product-category-2-dw-src-id !!product-category-2-dw-src-id
                        :product-product-name !!product-product-name
                        :order-dw-src-id ?order-dw-src-id :order-dw-dt ?order-dw-dt
                        :order-item-id !j-product-dw-id
                        :order-item-quantity ?order-item-quantity :order-item-price ?order-item-price :order-item-warehouse-id ?order-item-warehouse-id
                        :order-item-system-discount-amount ?order-item-system-discount-amount :order-item-tax-amount ?order-item-tax-amount
                        :order-item-logistics-amount ?order-item-logistics-amount
                        :coupon-id ?coupon-id :coupon-discount-amount ?coupon-discount-amount
                        :event-dw-src-id !j-event-dw-src-id :event-type-name !!event-type-name :event-event-name !!event-event-name
                        :show-dw-id !j-show-dw-id :show-dw-src-id !show-dw-src-id
                        :show-show-name !!show-show-name :show-begin-ts !!show-begin-ts :show-end-ts !!show-end-ts
                        :preview-show-dw-id !j-preview-show-dw-id :preview-show-dw-src-id !preview-show-dw-src-id
                        :preview-show-show-name !!preview-show-show-name :preview-show-begin-ts !!preview-show-begin-ts :preview-show-end-ts !!preview-show-end-ts
                        :replay-show-dw-id !j-replay-show-dw-id :replay-show-dw-src-id !replay-show-dw-src-id
                        :replay-show-show-name !!replay-show-show-name :replay-show-begin-ts !!replay-show-begin-ts :replay-show-end-ts !!replay-show-end-ts
                        :> ?user-tkv)
              (pivot-single ?user-tkv :> ?user-tkvs)
              )
        $
        (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order"
                         :outfields ["?user-id" "?user-tkvs"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :compression  :enable) $)))
  )
