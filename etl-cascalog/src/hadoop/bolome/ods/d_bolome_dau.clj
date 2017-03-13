(ns hadoop.bolome.ods.d_bolome_dau
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile hfs-textline]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]
            [taoensso.timbre :refer [info debug warn set-level!]])
  (:gen-class))

(set-level! :warn)

(defn -main []
  (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
             ?dt ?dau]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_dau" :skip-header? true :delimiter ",")
             :> ?rn ?dt ?dau)
            (identity ?dt :> ?dw-dt)
            (str ?dw-dt "T00:00:00+0000" :> ?dw-ts)
            (identity "" :> ?dw-src-id))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_dau"
                       :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                   "?dt" "?dau"]
                       :delimiter "\001"
                       :templatefields ["?dw-dt"]
                       :sinkmode :replace
                       :sink-template "p_dw_dt=%s"
                       :compression  :enable) $)) )
