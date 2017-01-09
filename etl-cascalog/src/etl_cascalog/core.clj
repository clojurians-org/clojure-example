(ns etl-cascalog.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.periodic :refer [periodic-seq]]
            [instar.core :refer [transform get-in-paths expand-path]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]] )
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]) )

(set-level! :info)

(defn parse-dt [dt] (tf/parse (tf/formatter "yyyy-MM-dd") dt) )
(defn unparse-dt [dt-obj] (tf/unparse (tf/formatter "yyyy-MM-dd") dt-obj))
(defn tomorrow-dt [dt] (-> (tf/parse (tf/formatter "yyyy-MM-dd") dt) (t/plus (t/days 1)) unparse-dt) )
(defn future-dt [] (-> (t/now) unparse-dt tomorrow-dt))
(t/today)
(defn last-day [dt]        (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (last-day-of-the-month- it) (unparse-dt it)) )
(defn prev-last-day [dt]   (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (t/plus it (t/days -1)) (unparse-dt it)) ) 
(defn prev-last-month [dt] (as-> (subs dt 0 4) it (tf/parse (tf/formatter "yyyy") it) (t/plus it (t/days -1)) (unparse-dt it)) )

(defn mk-dts [start-dt end-dt]
  (let [[start-dt-obj end-dt-obj] (map #(->>  (subs % 0 10)  (tf/parse (tf/formatter "yyyy-MM-dd"))) [start-dt end-dt])]
    (vector (map #(->> % (tf/unparse (tf/formatter "yyyy-MM-dd")))
                 (periodic-seq start-dt-obj (t/plus end-dt-obj (t/days 1))  (t/days 1))) )) )
(defn mk-month-dts [start-dt end-dt]
  (vector (map #(->> % (tf/unparse (tf/formatter "yyyy-MM-dd")) last-day)
               (periodic-seq (parse-dt start-dt) (t/plus (parse-dt end-dt) (t/days 1)) (t/months 1)))) )

(defaggregatefn collect-kv ([] {}) ([acc k v] (assoc acc k v)) ([x] [x]) )
(defn vars->kv [header] (mapfn [& coll] (zipmap header coll)))
(defn vars->pair [header] (mapfn [& coll] [(map vector header coll)] ))

(defn node->code [x]  (some->> x str (re-find #"\[(.*)]") second))
(defn map->code-map [x] (->> x (map (fn [[k v]] [(node->code k) v])) (into {})))
(defn kv->trgx [trgx]
  (mapfn [m] (clojure.walk/postwalk
              (fn [x] (match x [(node :guard #(contains? (map->code-map m) (node->code %))) node-attrs]
                             [node (merge-with merge node-attrs {:DATA (m node)})] :else x) )
              trgx)) )

(comment
  (re-find #"\[(.*)]")
  (def data {"[x]a" {:new-name "aa"}
             "[y]b" {:new-name "bb"}})
  
  (def tr {"[x]a" {:DATA {:name "a"}
               :CHILDREN {"[y]b" {:DATA {:name "b"}
                              :CHILDREN {}} }}})
  ((kv->trgx tr) data) 
  )

(defn kv->tuple [ks] (mapfn [m] (mapv m ks)))
(defmapfn kv->lkp [m & ks] (mapv m ks))
(defmapcatfn split-rows [x] x)
(defaggregatefn str-max
  ([] nil) ([x] [x])
  ([acc str] (if (pos? (compare str acc)) str acc )))

(defn tr-dimension-metrics [dimension-header metrics-header]
  (aggregatefn ([] {}) ([x] [x])
               ([acc & coll] (let [[dimension metrics] (split-at (count dimension-header) coll)
                                   dimension-pair (mapv vector dimension-header dimension)
                                   metrics-pair (mapv vector metrics-header metrics)]
                               (reduce #(assoc-in %1 (conj dimension-pair (first %2)) (second %2)) {} metrics-pair) ))))
(defmapfn pair-edn->json [pair-tree]
  (-> (clojure.walk/prewalk #(match [%]  [([(k :guard keyword?)  v] :guard (complement (partial instance? java.util.Map$Entry))) ]  (str (name k) "=" v) :else %) pair-tree) generate-string))

(defn pg-tap [db tabname header]
  (new JDBCTap (str "jdbc:postgresql://192.168.1.3:5432/" db
                    "?useSSL=true&ssl=true&characterEncoding=utf-8&stringtype=unspecified&sslmode=require&sslkey=/data/ssl/client/client.key.pk8&sslcert=/data/ssl/client/client.cert.pem&sslrootcert=/data/ssl/client/root.cert.pem&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory")
       "ms"
       "spiderdt"
       "org.postgresql.Driver"
       tabname
       (new JDBCScheme
            nil, nil
            (new Fields (into-array (map (partial str "?") header))
                 (into-array (repeat (count header) String)))
            (into-array (map #(clojure.string/replace % #"-" "_") header))
            nil, nil, -1, nil, nil, false)))

(defn report->next-dt [report-tap-out report]
  [(or (some-> (??<- [?dw-dt-max]
                     ((select-fields report-tap-out ["?dw-dt" "?report"] ) :> ?dw-dt report)
                     (str-max ?dw-dt :> ?dw-dt-max))
               ffirst tomorrow-dt)
       "1970-01-01")
   (future-dt) ] )

(defn detele-report! [report-tap-out report-name start-dt end-dt]
  (.executeUpdate report-tap-out (format  "DELETE FROM report WHERE project = 'cocacola' AND category = 'score' AND report = '%s' AND dw_dt BETWEEN '%s' AND '%s'" report-name start-dt end-dt) ) )

(def trgx-kpi (-> (??<- [?data] ((pg-tap "dw" "conf.trgx_cocacola" ["key" "data" "dw_in_use" "dw_ld_ts"]) :> "KPI" ?data "1" _)) ffirst read-string) )

(defn trgx->leaf [trgx]
  (->> trgx (tree-seq map? #(interleave (keys %) (vals %))) rest (partition 2) (filter #(= (:CHILDREN (second %)) {})) (map vec) (into {})))

(defn trgx->path [trgx]
  (->>  trgx extract-paths (filter #(= (last %) :DATA))  (map (comp (juxt last identity) (partial take-nth 2))) (into {}))  )
(defn path-trgx->path [trgx]
  (->> trgx extract-paths (filter #(= (last %) :DATA)) (map (comp (juxt last identity) butlast)) (into {})) )

(defn trgx-leaf-trunc [level trgx]
  (let [[trgx-leaf trgx-path] ((juxt trgx->leaf trgx->path) trgx)]
    (reduce (fn [acc [leaf-key leaf-value]]
              (assoc-in acc (as-> leaf-key $ (trgx-path $) (take level $) (vec $) (conj $ leaf-key))  leaf-value))
            {} trgx-leaf) ))

(defn trgx->kv [trgx]
  (->> trgx trgx->path vals (mapv #(vector % (get-in trgx (-> (interleave % (repeat :CHILDREN)) butlast)))) (into {})) )
(defn path-trgx->pair [trgx] (->> trgx path-trgx->path vals (mapv #(vector % (get-in trgx %)))))
(defn path-trgx->kv [trgx] (->> trgx path-trgx->pair (into {}))  )
(defn path-trgx->tuple-kv [header trgx] (->> trgx path-trgx->pair (map (fn [[path value]] (zipmap header (conj (vec path)  value)) ))) )

(comment
  (trgx->kv trgx-kpi)
  (path-trgx->tuple-kv [:channel :kpi :metrics :value] (trgx-leaf-trunc 2 trgx-kpi))
  (map count (path-trgx->tuple (trgx-leaf-trunc 2 trgx-kpi))) 
  )

(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-dts
  (<- [?mbd ?bg ?bottler ?channel ?code ?item ?fact ?dw-dts]
   (score-tap-in :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value)
   (collect-kv ?dw-dt ?value :> ?dw-dts)) )

(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value]
      (score-dts :> ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?dw-dts)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month) ?dw-dt :> !prev-last-day !prev-last-month)
      (kv->lkp ?dw-dts ?dw-dt !prev-last-day !prev-last-month :> ?value !pp-value !last-dec-value)) )

(defn score-trgx [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?trgx-data]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value)
      ((vars->kv [:value :pp-value :last-dec-value]) ?value !pp-value !last-dec-value :> ?values)
      (str "[" ?code "]" ?item :> ?code-item)
      (collect-kv ?code-item ?values :> ?code-item-values)
      ((kv->trgx trgx-kpi) ?code-item-values :> ?trgx-data) ))

(defn score-opportunity-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-trgx [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?trgx-data)
      (identity ["cocacola" "score" "opportunity" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :bottler]) ?dw-dt ?bottler :> ?selector-edn)
      ((tr-dimension-metrics [] [:trgx-data]) ?trgx-data :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics) ))

(defn score-channel_metrics_opportunity-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-trgx [start-dt end-dt] ) :> ?dw-dt ?bg ?bottler ?trgx-data)
      (identity ["cocacola" "score" "channel_metrics_opportunity" ""] :> ?project ?category ?report ?selector-desc)
      (trgx-leaf-trunc 1 ?trgx-data :> ?trgx-trunc-L1)
      ((c/comp split-rows (mapfn [x] (->> x (path-trgx->tuple-kv [:channel :metrics :node-value]) vector))) ?trgx-trunc-L1 :> ?node-kv)
      ((kv->tuple [:channel :metrics :node-value]) ?node-kv :> ?channel ?metrics ?node-value)
      ((vars->pair [:period :bottler]) ?dw-dt ?bottler :> ?selector-edn)
      ((kv->tuple [:DATA]) ?node-value :> ?node-data)
      ((kv->tuple [:c_total_score :c_weight :value :pp-value :last-dec-value]) ?node-data :> ?c_total_score ?c_weight ?value !pp-value !last-dec-value)
      ((tr-dimension-metrics [:metrics] [:channel :c_total_score :c_weight :value :pp-value :last-dec-value])
           ?metrics ?channel ?c_total_score ?c_weight ?value !pp-value !last-dec-value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

(defn score-bottler_ranking-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-trgx [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?trgx-data)
      (identity ["cocacola" "score" "bottler_ranking" ""] :> ?project ?category ?report ?selector-desc)
      (trgx-leaf-trunc 2 ?trgx-data :> ?trgx-trunc-L2)
      ((c/comp split-rows (mapfn [x] (->> x (path-trgx->tuple-kv [:channel :kpi :metrics :node-value]) vector))) ?trgx-trunc-L2 :> ?node-kv)
      ((kv->tuple [:channel :kpi :metrics :node-value]) ?node-kv :> ?channel ?kpi ?metrics ?node-value)
      ((vars->pair [:period :channel :kpi :metrics]) ?dw-dt ?channel ?kpi ?metrics :> ?selector-edn)
      ((kv->tuple [:DATA]) ?node-value :> ?node-data)
      ((kv->tuple [:c_total_score :c_weight :value :pp-value :last-dec-value]) ?node-data :> ?c_total_score ?c_weight ?value !pp-value !last-dec-value)
      ((tr-dimension-metrics [:bg :bottler] [:c_total_score :c_weight :value :pp-value :last-dec-value]) ?bg ?bottler ?c_total_score ?c_weight ?value !pp-value !last-dec-value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics) ) )

(defn -main []
  #_(detele-report! report-tap-out "opportunity" ["2016-01-01" "2016-01-01"] )
  #_(??- (score-channel_metrics_opportunity-report (report->next-dt report-tap-out "channel_metrics_opportunity")))
  #_(?- report-tap-out (score-opportunity-report ["2016-01-01" "2016-01-01"]))
  
  (?- report-tap-out (score-opportunity-report (report->next-dt report-tap-out "opportunity")))
  (?- report-tap-out (score-channel_metrics_opportunity-report (report->next-dt report-tap-out "channel_metrics_opportunity")))
  (?- report-tap-out (score-bottler_ranking-report (report->next-dt report-tap-out "bottler_ranking")))
  )




(comment

  )
