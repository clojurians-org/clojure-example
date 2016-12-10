(ns cascalog-etl.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn mapcatfn aggregatefn]]
            [cascalog.logic.def :refer [defmapcatfn]]
            [cascalog.logic.ops :as c ]
            [cascalog.cascading.operations :refer [rename*]]
            [cascalog.playground :refer [sentence person age]]
            [dk.ative.docjure.spreadsheet :refer
            [load-workbook select-sheet select-columns
              sheet-seq row-seq cell-seq read-cell]]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clojure.core.reducers :as r]
            [clojure.data.json :as json]
            [clojure.walk :refer [prewalk postwalk]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as f])
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
       (into [])))

(def score-dts
  (<- [?dmbd ?bg ?bottler ?channel ?code ?item ?fact ?period-values]
      (score  :> ?period ?dmbd ?bg ?bottler ?channel ?code ?item ?fact ?value)
      ((mapfn [it] (->> it int str
                        (f/parse (f/formatter "yyyyMM"))
                        last-day-of-the-month-
                        (f/unparse (f/formatter "yyyy-MM-dd"))))
           ?period :> ?period-fmt)
      ((aggregatefn ([] {})
                    ([acc k v] (assoc acc k v))
                    ([x] [x]))
       ?period-fmt ?value :> ?period-values) ))

(def score-window
  (<- [?period ?dmbd ?bg ?bottler ?channel ?code ?item ?fact ?value !last-dec-value !pp-value]
      (score-dts  :> ?dmbd ?bg ?bottler ?channel ?code ?item ?fact ?period-values)
      ((mapcatfn [it] (mapv #(cons % (mapv it [%
                                               (as-> % $ (subs $ 0 7)
                                                     (f/parse (f/formatter "yyyy-MM") $)
                                                     (t/plus $ (t/days -1))
                                                     (f/unparse (f/formatter "yyyy-MM-dd") $))
                                               (as-> % $ (subs $ 0 4)
                                                     (f/parse (f/formatter "yyyy") $)
                                                     (t/plus $ (t/days -1))
                                                     (f/unparse (f/formatter "yyyy-MM-dd") $))
                                               ])) (keys it)))
       ?period-values :> ?period ?value !pp-value !last-dec-value)) )

(def score-channel
  (<- [?period ?selector !dimension-metrics]
      ; ["Availability / 产品铺货", "SOVI / 排面占有率", "Cooler / 冰柜", "Activation / 渠道活动", "价格沟通.*"]
      (score-window  :> ?period ?dmbd ?bg ?bottler ?channel ?code ?channel "Score" ?value !pp-value !last-dec-value)
      (((fn [header] (mapfn [& coll] [(mapv vector header coll)] ))
           [:period :bg :bottler]) ?period ?bg ?bottler :> ?selector)
      (((fn [[dimension-header metrics-header]]
          (aggregatefn ([] {})
                       ([acc & coll] (let [[dimension metrics] (split-at (count dimension-header) coll)
                                           dimension-pair (mapv vector dimension-header dimension)
                                           metrics-pair (mapv vector metrics-header metrics) ]
                                       (reduce #(assoc-in %1 (conj dimension-pair (first %2)) (second %2)) {} metrics-pair) ))
                       ([x] [x])))
        [[:channel :bg] [:score :last-dec-score :pp-score]]) ?channel ?bg ?value !pp-value !last-dec-value :> !dimension-metrics)) )

(defn json-format [pair-obj]
  (-> (prewalk #(if (and (sequential? %) (= (count %) 2)  (keyword? (first %)) (not (instance? java.util.Map$Entry %)) )
                  (str (-> % first name) "=" (-> % second str))
                 %) pair-obj) 
      (json/write-str :escape-unicode false :escape-slash false))  )

(def score-channel_bg-mysql
  (<- [?dw_dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-channel :> ?period ?selector-edn ?dimension-metrics-edn)
      (identity ?period :> ?dw_dt)
      (json-format ?selector-edn :> ?selector)
      (json-format ?dimension-metrics-edn :> ?dimension-metrics)
      (identity "cocacola" :> ?project)
      (identity "score" :> ?category)
      (identity "channel_bg" :> ?report)
      (identity "" :> ?selector-desc) ))

(defn mysql-tap [header]
  (new JDBCTap "jdbc:mysql://192.168.1.3:3306/ms?useSSL=false&characterEncoding=utf-8"
       "ms"
       "spiderdt"
       "com.mysql.jdbc.Driver"
       "report"
       (new JDBCScheme
            (new Fields (into-array (map (partial str "?") header))
                 (into-array (repeat (count header) String)))
            (into-array (map #(clojure.string/replace % #"-" "_") header) )) ) )

(comment
  (?- (mysql-tap ["dw_dt"  "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]) score-channel-mysql)
  )
