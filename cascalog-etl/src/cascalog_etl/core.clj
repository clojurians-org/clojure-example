(ns cascalog-etl.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defaggregatefn aggregatefn]]
            [cascalog.logic.def :refer [defmapcatfn]]
            [cascalog.logic.ops :as c ]
            [cascalog.cascading.operations :refer [rename*]]
            [cascalog.playground :refer [sentence person age]]
            [dk.ative.docjure.spreadsheet :refer
            [load-workbook select-sheet select-columns
              sheet-seq row-seq cell-seq read-cell]]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clojure.core.reducers :as r])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :info)
(def file-path "/home/spiderdt/work/git/spiderdt-team/var/data/cocacola/score.xls")

(def score
  (->> (load-workbook file-path)
       (sheet-seq)
       (r/mapcat #(-> % row-seq rest))
       (r/map cell-seq)
       (r/map #(map read-cell % ))
       (r/map #(->> % (partition 9 9 [0]) first vec))
       (into []) ))

(def score-channel
  (<- [?selector ?dimension-metrics]
      ; [period dmbd bg bottler channel code item  fact value]
      ; ["Availability / 产品铺货", "SOVI / 排面占有率", "Cooler / 冰柜", "Activation / 渠道活动", "价格沟通.*"]
      (score  :> ?period ?dmbd ?bg ?bottler ?channel ?code ?channel "Score" ?value)
      (((fn [header] (mapfn [& coll] [(mapv vector header coll)] ))
           [:period :bg :bottler]) ?period ?bg ?bottler :> ?selector)
      (((fn [header] (aggregatefn ([] {})
                                  ([acc & coll] (apply assoc-in acc ((juxt drop-last last) (mapv vector header coll))))
                                  ([x] [x])))
           [:channel :bg :score]) ?channel ?bg ?value :> ?dimension-metrics)  ))

(def score-channel-mysql
  (<- [?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (score-channel :> ?selector-edn ?dimension-metrics-edn)
       (str ?selector-edn :> ?selector)
       (str ?dimension-metrics-edn :> ?dimension-metrics)
       (identity "cocacola" :> ?project)
       (identity "score" :> ?category)
       (identity "channel_bg" :> ?report)
       (identity "" :> ?selector-desc) ))

(def mysql-tap
  (new JDBCTap "jdbc:mysql://192.168.1.3:3306/ms?useSSL=false"
       "ms"
       "spiderdt"
       "com.mysql.jdbc.Driver"
       "report"
       (new JDBCScheme
            (new Fields (into-array ["?project" "?category" "?report" "?selector" "?selector-desc" "?dimension-metrics"])
                 (into-array (repeat 6 String)))
            (into-array ["project" "category" "report" "selector" "selector_desc" "json"])) ) )

(?- mysql-tap score-channel-mysql)
