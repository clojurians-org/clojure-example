(ns highchart-ui.core
  (:require [om.core :as om :include-macros true]
            [om.dom :as dom :include-macros true]
            [sablono.core :as html :refer-macros [html]]))

(enable-console-print!)

(println "This text is printed from src/hello/core.cljs. Go ahead and edit it and see reloading in action.")

(defonce app-state (atom {:dimension ["Hyper" "Super" "GT" "E&D M/H" "E&D Trad"]
                          :metrics-vals [{:name "score" :data [80.7 70.6 46.2 29.8 23.7] :type :column}] }))

(defn gen-chart [cursor owner]
  (reify
    om/IRender
    (render [_]
      (html [:div {:id "gen-chart"}]))
    om/IDidMount
    (did-mount [_]
      (println "cursor" cursor)
      (when-let [[dimension metrics-vals] (mapv cursor [:dimension :metrics-vals])]
        (println "dimension" dimension "metrics-vals" metrics-vals)
        (.chart js/Highcharts "gen-chart"
                (clj->js {:title {:text ""}
                          :xAxis {:categories dimension}
                          :series metrics-vals}) )))))
(om/root
  (fn [data owner]
    (reify om/IRender
      (render [_]
        (html
         [:div
          [:h1 "HighChart chart"]
          (om/build gen-chart data)]))))
  app-state
  {:target (. js/document (getElementById "app"))})

(defn on-js-reload []
  ;; optionally touch your app-state to force rerendering depending on
  ;; your application
  ;; (swap! app-state update-in [:__figwheel_counter] inc)
)
