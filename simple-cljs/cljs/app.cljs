(ns app
  (:require-macros [dommy.core :refer [sel sel1]]
                   [hiccups.core :as hiccups :refer [html]]
                   [cljs.core.async.macros :refer [go]])
  (:require [clojure.browser.repl :as repl]
            [dommy.core :as dommy]
            [hiccups.runtime :as hiccupsrt]
            [garden.core :refer [css]]
            [garden.selectors :as s]
            [cljs-http.client :as http]
            [cljs.core.async :refer [<!]]
             cljsjs.bootstrap
             cljsjs.jquery-ui
             cljsjs.bootstrap-treeview))

(defonce conn
  (repl/connect "http://localhost:9000/repl"))

(enable-console-print!)

(defn load-dom []
  (dommy/set-html! (sel1 :body) 
    (html ["div#app" ["div#tree-data" {:style "display:none;"}] ["div#tree.col-sm-12"] 
                     ["div#table-data" {:style "display:none;"}] ["div#table.col-sm-12"]
                     ["div#form-data" {:style "display:none;"}] ["div#form.col-sm-12"]])))

(defn build-tree []
  (let [tree-edn [{:name "aa" :value "xx" :desc "aaa"}
                  {:name "bb" :value "yy" :desc "bbb"}
                  {:name "cc" :value "zz" :desc "ccc"}]
        _ (dommy/set-text! (sel1 :#tree-data) (pr-str tree-edn))
        tree-view-edn (map-indexed (fn [id row] {:text (goog.string.format "[%s]=%s" id  (pr-str row)) 
                                                 :nodes (mapv (fn [[k v]] {:text (str k "=" v)}) row)}) tree-edn)]
    (.treeview (js/jQuery "#tree") (clj->js {:data tree-view-edn}))
    (.treeview (js/jQuery "#tree") "collapseAll" (clj->js {:silent true}))  ))

(defn build-table []
  (let [table-edn [["aaa" "AAA" 50]
                   ["bbb" "BBB" 100]
                   ["ccc" "CCC" 70]]
        table-dom [:table.table.table-bordered.table-condensed
                   [:tbody (map-indexed (fn [id row] [:tr (map #(do [:td (str %)]) (cons id row))]) table-edn)]] ]
    (dommy/set-html! (sel1 :#table) (html table-dom) )  ))

(defn build-form []
  (let [form-edn ["aaa[AAA]" "bbb[BBB]" "ccc[CCC]"]
        _ (dommy/set-text! (sel1 :#form-data) (pr-str form-edn))
        form-dom [:div.row (map-indexed (fn [id row] [:div.form-group
                                                      [:label.control-label.col-sm-2 {:for (goog.string.format "row-%s" id)} (str row ":")]
                                                      [:input.col-sm-10 {:type "text" :id (goog.string.format "row-%s" id)}]])
                              form-edn)]]
    (dommy/set-html! (sel1 :#form) (html form-dom))  ))
(defn load-data []
  (build-tree)
  (build-table)
  (build-form))

(defn load-css []
  (dommy/set-html! (sel1 :head)
                   (html [:link {:rel "stylesheet" :href "ext/bootstrap-3.3.7-dist/css/bootstrap.min.css"}]
                         [:style {:type "text/css"} "\n"
                          (css [:#app {:margin "0 auto" :width "80%"}])])))

(defn load-event [])

(load-dom)
(load-data)
(load-css)
(load-event)
  

(comment
  #_(html ["tr" (seq [["td" "1"] ["td" "2"]])])
  #_(html ["tr" ["td" "1"] ["td" 2]])
  (css [:body {:font "16px sans-serif"}] [:h1 {30000 "nom-nom"}])
)
