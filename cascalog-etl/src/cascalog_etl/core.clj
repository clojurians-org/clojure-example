(ns cascalog-etl.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defaggregatefn aggregatefn]]
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

(defn mysql-tap []
  (let [scheme (JDBCScheme. (Fields. (into-array String ["" "screen_name" "content"])) (into-array String ["id" "screen_name" "content"]))
             table-desc (TableDesc. "tweets")
             tap (JDBCTap. "jdbc:mysql://localhost:3306/burn?user=root&password=" "com.mysql.jdbc.Driver" table-desc scheme)]
         tap))

(def sink-mysql [])
(?- (stdout) score-channel)

(def score-test [["a" "E&D M/H / 中高档餐" "G13" 25.1535054339554]
                 ["b"  "E&D Trad / 传统餐饮" "G18" 23.8869117480029] ] )
(?<- (stdout)
     [?selector ?dimension-metrics]
     (score-test :> ?selector ?channel ?bg ?value)
     (((fn [header] (aggregatefn ([] {})
                                 ([acc & coll] (apply assoc-in acc ((juxt drop-last last) (map vector header coll))))
                                 ([x] [x])))
       [:channel :bg :score]) ?channel ?bg ?value  :> ?dimension-metrics))
