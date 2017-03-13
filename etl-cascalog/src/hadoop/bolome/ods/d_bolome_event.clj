(ns hadoop.bolome.ods.d_bolome_event
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile hfs-textline]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]
            [taoensso.timbre :refer [info debug warn set-level!]])
  (:gen-class))

(set-level! :warn)

(defn -main []
  (as-> (<- [?dw-dt ?dw-ts ?dw-src-id
             ?event-id  ?type-name  ?event-name ?create-dt]
            ((hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_event" :skip-header? true :delimiter ",")
             :> ?event-id  ?type-name  ?event-name ?create-dt)
            (identity ?create-dt :> ?dw-dt)
            (str ?dw-dt "T00:00:00+0000" :> ?dw-ts)
            (identity ?event-id :> ?dw-src-id))
      $
    (?- (hfs-delimited "hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_event"
                       :outfields ["?dw-dt" "?dw-ts" "?dw-src-id"
                                   "?event-id" "?type-name" "?event-name" "?create-dt"]
                       :delimiter "\001"
                       :templatefields ["?dw-dt"]
                       :sinkmode :replace
                       :sink-template "p_dw_dt=%s"
                       :compression  :enable) $)) )
