(ns app
  (:require-macros [dommy.core :refer [sel sel1]]
                   [hiccups.core :as hiccups :refer [html]])
  (:require [dommy.core :as dommy]
            [cljs.reader :refer [read-string]]))

(enable-console-print!)

(defn load-dom [] (dommy/set-html! (sel1 :body) (html [:div#app [:div#chart-data {:style "display:none;"}] [:div#chart-container]])) )
(defn load-data []
  (when-let [b64-data (some-> (.-href js/location) (clojure.string/split #"#" 2) second (clojure.string/split #"=" 2) second)]
    (let [data-edn (-> b64-data js/atob read-string)
          _ (dommy/set-text! (sel1 :#chart-data) (pr-str data-edn))
          chart-edn (->> data-edn (mapv (fn [[k v-kv]] (mapv (fn [[v-k v-v]] {"x" k "y" v-v "z" v-k}) v-kv))) flatten)
          dimple-chart (doto (new js/dimple.chart (.newSvg js/dimple "#chart-container" 600 400) (clj->js chart-edn))
                         (.setBounds 60 30 505 305)
                         (-> (.addCategoryAxis "x" "x") (.addOrderRule "x"))
                          (.addMeasureAxis "y" "y")
                         (.addSeries "z" js/dimple.plot.line)
                         (.addLegend 60 10 500 20 "right"))]
      (.draw dimple-chart)) )
  )
(load-dom)
(load-data)
