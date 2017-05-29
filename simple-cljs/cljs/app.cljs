(ns app
  (:require-macros [dommy.core :refer [sel sel1]]
                   [hiccups.core :as hiccups :refer [html]]
                   [cljs.core.async.macros :refer [go]])
  (:require [clojure.browser.repl :as repl]
            [dommy.core :as dommy]
            [hiccups.runtime :as hiccupsrt]
            [cljs-http.client :as http]
            [cljs.core.async :refer [<!]]
             cljsjs.jquery-ui
             cljsjs.bootstrap-treeview))

(defonce conn
  (repl/connect "http://localhost:9000/repl"))

(enable-console-print!)

(defn load-dom []
  (dommy/set-html! (.-body js/document) 
    (html ["div#app"])))

(defn build-tree []
  (let [tree-edn [{:name "aa" :value "xx" :desc "aaa"}
                  {:name "bb" :value "yy" :desc "bbb"}]
        test-edn [{:text "node1"
                    :nodes [{:text "child1"}
                            {:text "child2"}]}]]
    (.treeview (js/jQuery "#app") (clj->js {:data test-edn})) ))

(defn load-data []
  (build-tree))

(load-dom)
(load-data)
  

(comment

)
