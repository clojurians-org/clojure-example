(ns web-rest-starter.core
  (:require
   [slingshot.slingshot :refer [try+ throw+]]
   [clojure.core.async :refer [<!! chan]]
   [org.httpkit.server :as httpkit-server]
   [com.stuartsierra.component :as component]
   [ring.middleware.defaults :refer [wrap-defaults]]
   [ring.middleware.cors :refer [wrap-cors]]
   [compojure.route :as route]
   [hiccup.core :as hiccup]
   [compojure.core :as comp :refer [defroutes GET POST ANY]]
   [compojure.route :as route :refer [resources]]
   [liberator.core :refer [defresource]]
   [cheshire.core :refer [generate-string]]))

(def ring-defaults-config
  (assoc-in ring.middleware.defaults/site-defaults
            [:security :anti-forgery]
            {:read-token (fn [req] (-> req :params :csrf-token))}))

#_(defn main-page []
  (hiccup/html
   [:head
    [:title "Storm Rest Project"]]
   [:body
    [:h1 "Storm Rest Project Here!"]]))


(def data
[
  {
    "chart" {
      "type" "line"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "column"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 11]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "column"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "line"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "line"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "line"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]},
  {
    "chart" {
      "type" "line"},
    "title" {
      "text" "知助测试图表"},
    "xAxis" {
      "categories" ["X1", "X2", "X3", "X4"]},
    "yAxis" {
      "title" {
        "text" "Y"}},
    "series" [{
      "name" "luyon",
      "data" [3, 5, 8, 6]}, {
      "name" "open",
      "data" [5, 7, 3 , 8]}]}]
)

(defresource hello-world
  :available-media-types ["application/json"]
  :handle-ok (fn [context] (generate-string {:status "ok" :data data})) )

(defroutes routes
  (GET "/" [] hello-world)
  (resources "/")
  (route/not-found "Page not found"))

(defrecord HttpServer [http-config]
  component/Lifecycle
  (start [{:keys [port] :as component}]
    (prn "Starting HTTP Server")
    (let [server (httpkit-server/run-server
                  (-> routes
                      (wrap-defaults ring-defaults-config)
                      (wrap-cors :access-control-allow-origin [#".*"]
                                 :access-control-allow-methods [:get :put :post :delete]))
                  {:port port})]
      (prn "Http-Kit server is running at http://0.0.0.0:" port)
      (assoc component :server server)))
  (stop [{:keys [server] :as component}]
    (try+ (server :timeout 100) (finally (prn "Game is over!")))
    (assoc component :server nil)
    (prn "Stopping HTTP Server")))

(defn get-system [{:keys [http-config]}]
  (component/system-map
   :http (component/using (map->HttpServer http-config) [])))

(defn -main []
  (component/start (get-system {:http-config {:port 8080}}))
    (<!! (chan)))
