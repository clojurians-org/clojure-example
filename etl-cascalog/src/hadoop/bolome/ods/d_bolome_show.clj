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

(ns bolome.ods.d-bolome-show
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]]
            [common.trgx :refer :all]
            [clojure.java.jdbc :as j])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

(defn merge-stg-ods-tmp [stg-tap-in ods-tap-out]
  #_(??- (merge-stg-ods-tmp stg-tap-in ods-tap-out))
  (<- [?dw-dt ?dw-ts ?dw-id ?show-id ?show-name ?begin-time ?end-time]
         (stg-tap-in :> ?join-show-id !!stg-show-name !!stg-begin-time !!stg-end-time)
         (identity [nil] :> !stg-dw-id)
         (convert-null !!stg-begin-time)
         ((c/each identity) !!stg-end-time :> !stg-dw-ts)
         (ts->dt !stg-dw-ts :> !stg-dw-dt)
         (ods-tap-out :> !!ods-dw-dt !!ods-dw-ts !!ods-dw-id ?join-show-id !!ods-show-name !!ods-begin-time !!ods-end-time)
         (or-tuple !stg-dw-id !!ods-dw-id !stg-dw-dt !!ods-dw-dt !stg-dw-ts !!ods-dw-ts
                   !!stg-show-name !!ods-show-name !!stg-begin-time !!ods-begin-time !!stg-end-time !!ods-end-time :>
                   !dw-id-merge ?dw-dt-merge ?dw-ts-merge ?show-name-merge ?begin-time-merge ?end-time-merge)
         (identity 0 :> ?prt-no)
         ((row-num {0 0} (load-max-dw-id ods-tap-out))
              ?prt-no !dw-id-merge ?dw-dt-merge ?dw-ts-merge ?join-show-id ?show-name-merge ?begin-time-merge ?end-time-merge :> ?dw-id-kv)
         (split-rows ?dw-id-kv :> ?dw-id-tuple)
         ((tkv-select [:dw-id :tuple]) ?dw-id-tuple :> ?dw-id ?tuple)
         (identity ?tuple :> ?dw-dt ?dw-ts ?show-id ?show-name ?begin-time ?end-time)) )

(defn -main []
  (def stg-tap-in (pg-tap "dw" "stg.d_bolome_show" ["show-id" "show-name" "begin-time" "end-time"]))
  (def ods-tap-out (pg-tap "dw" "ods.d_bolome_show" ["dw-dt" "dw-ts" "dw-id" "show-id" "show-name" "begin-time" "end-time"]))
  (def ods-tmp-tap-out (pg-tap "dw" "d_bolome_show_ods" ["dw-dt" "dw-ts" "dw-id" "show-id" "show-name" "begin-time" "end-time"]))

  (create-table-if ods-tap-out
                   [[:dw_dt "CHAR(10)"]
                    [:dw_ts "CHAR(24)"]
                    [:dw_id :INT]
                    [:show_id :TEXT]
                    [:show_name :TEXT]
                    [:begin_time :TEXT]
                    [:end_time :TEXT]] )
  (create-table-if ods-tmp-tap-out
                   [[:dw_dt "CHAR(10)"]
                    [:dw_ts "CHAR(24)"]
                    [:dw_id :INT]
                    [:show_id :TEXT]
                    [:show_name :TEXT]
                    [:begin_time :TEXT]
                    [:end_time :TEXT]])
  
  (prn {:dt-rng (save-and-load-rng-dt! stg-tap-in ["?begin-time" "?end-time"] identity)} "running...")
  (try (?- ods-tmp-tap-out (merge-stg-ods-tmp stg-tap-in ods-tap-out)) (catch Exception _))
  (replace-into-ods ods-tap-out ods-tmp-tap-out)
  )

(comment
  (-main)
  )
