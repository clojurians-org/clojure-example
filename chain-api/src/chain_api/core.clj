(ns chain-api.core
  (:require
   [clojure.tools.namespace.repl :refer [refresh] ]
   [slingshot.slingshot :refer [try+ throw+]]
   [clojure.core.async :refer [<!! chan]]
   [org.httpkit.server :as httpkit-server]
   [com.stuartsierra.component :as component]
   [ring.middleware.basic-authentication :refer [wrap-basic-authentication]]
   [ring.middleware.defaults :refer [wrap-defaults]]
   [ring.middleware.cors :refer [wrap-cors]]
   [compojure.route :as route]
   [hiccup.core :as hiccup]
   [compojure.core :as comp :refer [defroutes context GET POST ANY]]
   [compojure.route :as route :refer [resources]]
   [liberator.core :refer [defresource]]
   [cheshire.core :refer [generate-string]]))

(def ring-defaults-config
  (assoc-in ring.middleware.defaults/site-defaults
            [:security :anti-forgery]
            {:read-token (fn [req] (-> req :params :csrf-token))}))

(defn authenticated? [name pass]
  (and (= name "larluo@spiderdt.com")
       (= pass "spiderdt.com")))

(defn not-implemented []
  (pr-str ["not-implemented!"]))

;; {:GET [:index :show] :POST [:create] :PUT [:update] :DELETE [:delete]}
(defn cache-resource-show [args] {:status :forward})
(defn auth-token-show [args] {:status :ok})
(defn auth-token-create [args] {:status :ok})
(defn auth-acl-show [args] {:status :ok})
(defn dispatch-resource-show [args] {:status :success})
(defn chain-resource-show [args]
  (reduce #(if-not (contains? #{:success :error} (:status %1)) (merge %1 (-> %1 :args %2)) (reduced %1))
          {:args nil} [cache-resource-get auth-token-get auth-acl-get dispatch-resource-get]))

(defroutes public-handler
  (GET "/ping" [] (pr-str ["pong"]))
  )

(defroutes protected-handler
  (context "/chain-api-v1" []
           (GET "/resource" [] (chain-resource-show {})))
  (context "/cache-api-v1" []
           (GET "/resource" [] (cache-resource-get {})) )
  (context "/auth-api-v1" []
           (GET "/token" [] (auth-token-show {}))
           (POST "/token" [] (auth-token-create {}))
           (GET "/acl" [] (auth-acl-show {}))
           #_(GET "/client")
           #_(GET "/user"))
  (context "/dispatch-api-v1" []
           (GET "/resource" [] (dispatch-resource-show {}))))

(defroutes handler public-handler (wrap-basic-authentication protected-handler authenticated?))

(defrecord HttpServer [http-config]
  component/Lifecycle
  (start [{:keys [port] :as component}]
    (prn "Starting HTTP Server")
    (let [server (httpkit-server/run-server (-> handler
                                                (wrap-defaults ring-defaults-config)
                                                (wrap-cors :access-control-allow-origin [#".*"]
                                                           :access-control-allow-methods [:get :put :post :delete]))
                                            {:port port})]
      (prn (format "Http-Kit server is running at http://0.0.0.0:%s" port))
      (assoc component :server server)))
  (stop [{:keys [server] :as component}]
    (try+ (server :timeout 100) (finally (prn "Game is over!")))
    (assoc component :server nil)
    (prn "Stopping HTTP Server")))

(defn get-system [{:keys [http-config]}]
  (-> (component/system-map
       :http (map->HttpServer http-config))
      (component/system-using {})))

(def system nil)
(defn init [] (alter-var-root #'system (constantly (get-system {:http-config {:port 1111}}) )))
(defn start [] (alter-var-root #'system component/start))
(defn stop [] (alter-var-root #'system (fn [s] (when s (component/stop s)))))
(defn go [] (init) (start))
(defn reset [] (stop) (refresh :after 'user/go))

(defn -main [] (go) (<!! (chan)))

(comment
  (init)
  (start)
  (stop)
  )
