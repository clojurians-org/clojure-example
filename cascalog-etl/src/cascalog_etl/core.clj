(ns cascalog-etl.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout]]
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

(def score-channel
  (<- [?dimensions ?channel ?value]
      ; [period dmbd bg bottler channel code item  fact value]
      (score :> ?period ?dmbd ?bg ?bottler ?channel ?code ?channel "Score" ?value)
      (vector :< ?period ?bg ?bottler :>> ?dimensions)))

(def data [[1 11 111] [2 22 222]])

(?<- (stdout)
     [?multi]
     (data ?one ?two ?three)
     (list :< ?one ?two ?three :> ?multi)
     )

(?- (stdout) score-channel)

(first (range 1 10))

(#'inc 1)
