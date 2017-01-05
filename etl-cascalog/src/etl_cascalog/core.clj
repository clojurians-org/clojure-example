(ns etl-cascalog.core
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn]]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [instar.core :refer [transform get-in-paths get-values-in-paths]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]) )

(set-level! :info)

(defn last-day [dt]        (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (last-day-of-the-month- it) (tf/unparse (tf/formatter "yyyy-MM-dd") it)) )
(defn prev-last-day [dt]   (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (t/plus it (t/days -1)) (tf/unparse (tf/formatter "yyyy-MM-dd") it)) ) 
(defn prev-last-month [dt] (as-> (subs dt 0 4) it (tf/parse (tf/formatter "yyyy") it) (t/plus it (t/days -1)) (tf/unparse (tf/formatter "yyyy-MM-dd") it)) )
(defaggregatefn kv-agg ([] {}) ([acc k v] (assoc acc k v)) ([x] [x]) )
(defn vars->map [header] (mapfn [& coll] (zipmap header coll)))
(defn map->tree [trgx] (mapfn [m] (clojure.walk/postwalk #(match % [(node :guard (partial contains? m)) node-attrs] [node (merge-with merge node-attrs {:DATA (m node)})] :else %) trgx)))
(defn map->tuple [coll] (mapfn [m] (mapv m coll)))

(defmapfn pair-tree->json [pair-tree]
  (-> (clojure.walk/prewalk #(match % [k v] (str k "=" v) :else %) pair-tree) generate-string))



(defn pg-tap [tabname header]
  (new JDBCTap "jdbc:postgresql://192.168.1.3:5432/dw?useSSL=true&ssl=true&characterEncoding=utf-8&stringtype=unspecified&sslmode=require&sslkey=/data/ssl/client/client.key.pk8&sslcert=/data/ssl/client/client.cert.pem&sslrootcert=/data/ssl/client/root.cert.pem&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory"
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

(def trgx-kpi (-> (??<- [?data] ((pg-tap "conf.trgx_cocacola" ["key" "data" "dw_in_use" "dw_ld_ts"]) :> "KPI" ?data "1" _)) ffirst read-string) )


(def score-tap (pg-tap "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value"]))

(defn code-pair [coll]
  (let [[kpi-name rest] ((juxt last butlast) coll)]
    [(->> kpi-name (re-find #"\[(.*)\]") second) (map #(clojure.string/replace % #"\[(.*)\]" "") rest)] ))

(def code-kv
  (->> [[* :CHILDREN * :DATA] [* :CHILDREN * :CHILDREN * :DATA] [* :CHILDREN * :CHILDREN * :CHILDREN * :DATA]]
       (map #(->> % (get-in-paths trgx-kpi) (map (comp code-pair (partial take-nth 2) first)) (into {})))
       (apply merge) ) )

(def score-dts
  (<- [?mbd ?bg ?bottler ?channel ?code ?item ?fact ?dw-dts]
   (score-tap :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value)
   (kv-agg ?dw-dt ?value :> ?dw-dts)) )

(defn score-sliding  [dt]
  (<- [?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value]
      (score-dts :> ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?dw-dts)
      (identity dt :> ?dw-dt)
      ( (map->tuple ((juxt identity prev-last-day prev-last-month) dt)) ?dw-dts :> ?value !pp-value !last-dec-value)) )

(defn score-tree [dt]
  (<- [?dw-dt ?bg ?bottler ?trgx-data]
      ((score-sliding dt) :> ?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value)
      ((vars->map [:value :pp-value :last-dec-value]) ?value !pp-value !last-dec-value :> ?values)
      (str "[" ?code "]" ?item :> ?code-item)
      (kv-agg ?code-item ?values :> ?code-item-values)
      ((map->tree trgx-kpi) ?code-item-values :> ?trgx-data)
      ) )

(??- (score-tree "2015-12-31"))

(comment
  (?<- (stdout)
       [?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value]
       ((score-sliding "2015-12-31") :> ?dw-dt ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value !pp-value !last-dec-value)
       (= ?mbd "CBL Jiangxi ED Mid/High"))
  )
