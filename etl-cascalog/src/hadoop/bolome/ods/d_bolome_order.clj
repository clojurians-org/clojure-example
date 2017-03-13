(ns hadoop.bolome.ods.d_bolome_order
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile hfs-textline]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]
            [taoensso.timbre :refer [info debug warn set-level!]])
  (:gen-class))

(set-level! :warn)

(defn -main []
  (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
             ?pay-dt ?user-id ?order-id ?barcode
             ?quantity ?price ?warehouse-id ?show-id ?preview-show-id ?replay-show-id ?coupon-id ?event-id
             ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_order" :skip-header? true :delimiter ",")
             :> ?pay-dt ?user-id ?order-id ?barcode
                ?quantity ?price ?warehouse-id ?show-id ?preview-show-id ?replay-show-id ?coupon-id ?event-id
             ?copon-discount-amount ?system-discount-amount ?tax-amount ?logistics-amount)
            (identity ?pay-dt :> ?dw-dt)
            (str ?dw-dt "T00:00:00+0000" :> ?dw-ts)
            (identity ?order-id :> ?dw-src-id))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_order"
                       :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                   "?pay-dt" "?user-id" "?order-id" "?barcode"
                                   "?quantity" "?price" "?warehouse-id" "?show-id" "?preview-show-id" "?replay-show-id" "?coupon-id" "?event-id"
                                   "?copon-discount-amount" "?system-discount-amount" "?tax-amount" "?logistics-amount"]
                       :delimiter "\001"
                       :templatefields ["?dw-dt"]
                       :sinkmode :replace
                       :sink-template "p_dw_dt=%s"
                       :compression  :enable) $)) )
