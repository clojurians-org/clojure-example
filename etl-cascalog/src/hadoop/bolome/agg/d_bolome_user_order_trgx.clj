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

(ns hadoop.bolome.agg.d_bolome_user_order_trgx
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

(defn latest-schema []
      {:DATA {:user-id "user-id"}
             :CHILDREN {"dw-dt" {:DATA {:dw-dt "dw-dt"}
                                 :CHILDREN {"product-dw-id" {:DATA {:product-dw-id "product-dw-id"
                                                                    :product-dw-src-id "product-dw-src-id"
                                                                    :product-category-1-dw-id "product-category-1-dw-id"
                                                                    :product-category-1-dw-src-id "product-category-1-dw-src-id"
                                                                    :product-category-2-dw-id "product-category-2-dw-id"
                                                                    :product-category-2-dw-src-id "product-category-2-dw-src-id"
                                                                    :product-product-name "product-product-name"}
                                                             :CHILDREN {"order-dw-src-id" {:DATA {:order-dw-src-id "order-dw-src-id"
                                                                                                  :order-dw-dt "order-dw-dt"}
                                                                                           :CHILDREN {"order-item-id" {:DATA {:order-item-id "order-item-id"
                                                                                                                               :order-item-quantity "order-item-quantity"
                                                                                                                               :order-item-price "order-item-price"
                                                                                                                               :order-item-warehouse-id "order-item-warehouse-id"
                                                                                                                               :order-item-system-discount-amount "order-item-system-discount-amount"
                                                                                                                               :order-item-tax-amount "order-item-tax-amount"
                                                                                                                               :order-item-logistics-amount "order-item-logistics-amount"}
                                                                                                                        :BRANCH {:coupon {"coupon-id" {:DATA {:coupon-id "coupon-id"
                                                                                                                                                              :coupon-discount-amount "coupon-discount-amount"}
                                                                                                                                                       :CHILDREN {}}}
                                                                                                                                 :event {"event-dw-src-id" {:DATA {:event-dw-src-id "event-dw-src-id"
                                                                                                                                                                   :event-type-name "event-type-name"
                                                                                                                                                                   :event-event-name "event-event-name"}
                                                                                                                                                            :CHILDREN {}}}
                                                                                                                                 :show {"show-dw-id" {:DATA {:show-dw-id "show-dw-id"
                                                                                                                                                             :show-dw-src-id "show-dw-src-id"
                                                                                                                                                             :show-show-name "show-show-name"
                                                                                                                                                             :show-begin-ts "show-begin-ts"
                                                                                                                                                             :show-end-ts "show-end-ts"}
                                                                                                                                                      :CHILDREN {}}}
                                                                                                                                 :preview-show {"preview-show-dw-id" {:DATA {:preview-show-dw-id "preview-show-dw-id"
                                                                                                                                                                             :preview-show-dw-src-id "preview-show-dw-src-id"
                                                                                                                                                                             :preview-show-show-name "preview-show-show-name"
                                                                                                                                                                             :preview-show-begin-ts "preview-show-begin-ts"
                                                                                                                                                                             :preview-show-end-ts "preview-show-end-ts"}
                                                                                                                                                                      :CHILDREN {}}}
                                                                                                                                 :replay-show {"replay-show-dw-id" {:DATA {:replay-show-dw-id "replay-show-dw-id"
                                                                                                                                                                           :replay-show-dw-src-id "replay-show-dw-src-id"
                                                                                                                                                                           :replay-show-show-name "replay-show-show-name"
                                                                                                                                                                           :replay-show-begin-ts "replay-show-begin-ts"
                                                                                                                                                                           :replay-show-end-ts "replay-show-end-ts"}
                                                                                                                                                                    :CHILDREN {}}}}}}}}}}}}})

(defn deep-merge [& vals]  (if (every? map? vals)  (apply merge-with deep-merge vals)  (last vals)))
(defn realize-trgx [schema tkvs]
  (->> tkvs
       clojure.edn/read-string
       (map #(clojure.walk/prewalk (fn [node] (if (string? node) (-> node keyword %) node))  schema))
       (apply deep-merge))  )

(defn -main []
  (with-job-conf {"mapreduce.input.fileinputformat.input.dir.recursive" "true"
                  "mapred.reduce.tasks" "1"
                  "mapred.job.map.memory.mb" "1024"
                  "mapred.job.reduce.memory.mb" "1024"
                  "mapred.compress.map.output" "true"
                  "mapred.map.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec"
                  "mapred.output.compression.codec" "org.apache.hadoop.io.compress.BZip2Codec" }
    (as-> (<- [?user-id ?user-trgx]
              ((hfs-textline "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order") :> ?row)
              (clojure.string/split ?row #"\001" 2 :> ?user-id ?user-tkvs)
              (realize-trgx (latest-schema)  ?user-tkvs :> ?user-trgx)
              )
        $
      (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/agg.db/d_bolome_user_order_trgx"
                         :outfields ["?user-id" "?user-trgx"]
                         :delimiter "\001"
                         :quote ""
                         :sinkmode :replace
                         :compression  :enable) $)))
  )
