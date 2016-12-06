(ns cascalog-etl.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn defmapop mapfn]]
            [cascalog.logic.def :refer [defmapcatfn]]
            [cascalog.logic.ops :as c ]
            [cascalog.playground :refer [sentence person age]]
            [dk.ative.docjure.spreadsheet :refer
             [load-workbook select-sheet select-columns
              sheet-seq row-seq cell-seq read-cell]]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clojure.core.reducers :as r]))

(set-level! :info)
(def file-path "/home/spiderdt/work/git/spiderdt-team/var/data/cocacola/score.xls")

(def score
  (->> (load-workbook file-path)
       (sheet-seq)
       (r/mapcat #(-> % row-seq rest))
       (r/map cell-seq)
       (r/map #(map read-cell % ))
       (r/map #(->> % (partition 9 9 [0]) first vec))
       (into [])))

(defmapop square [&coll] (map #(hash-map (name coll) coll) coll))


(def score-channel
  (<- [?selector ?dimension-metrics]
      ; [period dmbd bg bottler channel code item  fact value]
      ; ["Availability / 产品铺货", "SOVI / 排面占有率", "Cooler / 冰柜", "Activation / 渠道活动", "价格沟通.*"]
      (score :> ?period ?dmbd ?bg ?bottler ?channel ?code ?channel "Score" ?value)
      ((mapfn [header & coll] (map vector header coll))
           [:period :bg :bottler] ?period ?bg ?bottler :> ?selector)
      ((aggregatefn ([] {})
                    ([acc val] )
                    ([x] [x]))
           [:channel :bg :value] ?channel ?bg ?value :> ?dimension-metrics)  ))

(?- (stdout) score-channel)

