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

(ns bolome.ods.d-bolome-product_category
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
  (<- [?dw-dt ?dw-ts ?dw-id ?barcode ?product-name ?category-1 ?category-2]
         (stg-tap-in :> ?join-barcode !!stg-product-name !!stg-category-1 !!stg-category-2)
         (identity [nil] :> !stg-dw-id)
         (identity (latest-ts) :> !stg-dw-ts)
         (ts->dt !stg-dw-ts :> !stg-dw-dt)
         (ods-tap-out :> !!ods-dw-dt !!ods-dw-ts !!ods-dw-id ?join-barcode !!ods-product-name !!ods-category-1 !!ods-category-2)
         (or-tuple !stg-dw-id !!ods-dw-id !stg-dw-dt !!ods-dw-dt !stg-dw-ts !!ods-dw-ts
                   !!stg-product-name !!ods-product-name !!stg-category-1 !!ods-category-1 !!stg-category-2 !!ods-category-2 :>
                   !dw-id-merge ?dw-dt-merge ?dw-ts-merge ?product-name-merge ?category-1-merge ?category-2-merge)
         (identity 0 :> ?prt-no)
         ((row-num {0 0} (load-max-dw-id ods-tap-out))
              ?prt-no !dw-id-merge ?dw-dt-merge ?dw-ts-merge ?join-barcode ?product-name-merge ?category-1-merge ?category-2-merge :> ?dw-id-kv)
         (split-rows ?dw-id-kv :> ?dw-id-tuple)
         ((tkv-select [:dw-id :tuple]) ?dw-id-tuple :> ?dw-id ?tuple)
         (identity ?tuple :> ?dw-dt ?dw-ts ?barcode ?product-name ?category-1 ?category-2)) )

(defn -main []
  (def stg-tap-in (pg-tap "dw" "stg.d_bolome_product_category" ["barcode" "product-name" "category-1" "category-2"]))
  (def ods-tap-out (pg-tap "dw" "ods.d_bolome_product_category" ["dw-dt" "dw-ts" "dw-id" "barcode" "product-name" "category-1" "category-2"]))
  (def ods-tmp-tap-out (pg-tap "dw" "d_bolome_product_category_ods" ["dw-dt" "dw-ts" "dw-id" "barcode" "product-name" "category-1" "category-2"]))

  (create-table-if ods-tap-out
                   [[:dw_dt "CHAR(10)"]
                    [:dw_ts "CHAR(24)"]
                    [:dw_id :INT]
                    [:barcode :TEXT]
                    [:product_name :TEXT]
                    [:category_1 :TEXT]
                    [:category_2 :TEXT]] )
  (create-table-if ods-tmp-tap-out
                   [[:dw_dt "CHAR(10)"]
                    [:dw_ts "CHAR(24)"]
                    [:dw_id :INT]
                    [:barcode :TEXT]
                    [:product_name :TEXT]
                    [:category_1 :TEXT]
                    [:category_2 :TEXT]])
  
  (prn {:dt-rng (save-and-load-rng-dt! stg-tap-in :latest identity)} "running...")
  (try (?- ods-tmp-tap-out (merge-stg-ods-tmp stg-tap-in ods-tap-out)) (catch Exception _))
  (replace-into-ods ods-tap-out ods-tmp-tap-out)
  )

(comment
  (-main)
  )
