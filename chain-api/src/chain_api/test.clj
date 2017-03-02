(ns chain-api.core
  (:require
   [clojure.tools.namespace.repl :refer [refresh] ]
   [slingshot.slingshot :refer [try+ throw+]]
   [clojure.core.async :refer [<!! chan]]
   [org.httpkit.server :as httpkit-server]
   [org.httpkit.client :as httpkit-client]
   [com.stuartsierra.component :as component]
   [ring.middleware.params :refer [wrap-params]]
   [ring.middleware.basic-authentication :refer [wrap-basic-authentication]]
   [ring.middleware.defaults :refer [wrap-defaults]]
   [ring.middleware.cors :refer [wrap-cors]]
   [compojure.route :as route]
   [hiccup.core :as hiccup]
   [compojure.core :as comp :refer [defroutes context GET POST ANY]]
   [compojure.route :as route :refer [resources]]
   [clojure.data.codec.base64 :as base64]
   [cheshire.core :refer [generate-string parse-stream parse-string]]
   [clojure.java.jdbc :as j]
   [clj-time.core :as t :refer [last-day-of-the-month-]]
   [clj-time.format :as tf]
   [honeysql.core :as sql]
   [honeysql.helpers :as sqlh :refer [select from where defhelper collify values insert-into]]
   [honeysql.format :refer [format-clause to-sql]]
   [honeysql-postgres.format :refer :all]
   [honeysql-postgres.helpers :refer [with-columns]]
   #_[ring.middleware.json :refer [wrap-json-body]]
   [buddy.hashers :as hashers]))

(defn future-ts [seconds] (tf/unparse (-> (tf/formatter "yyyy-MM-dd'T'HH:mm:ssZ") (.withZone (t/default-time-zone))) (t/plus (t/now) (t/seconds seconds)) ) )
(defn latest-ts [] (future-ts 0))

(def pg-spec {:dbtype "postgresql"
              :dbname "ms"
              :host "192.168.1.3"
              :user "ms"
              :password "spiderdt"
              :sslmode "require"
              :sslkey "/data/ssl/client/client.key.pk8"
              :sslcert "/data/ssl/client/client.cert.pem"
              :sslrootcert "/data/ssl/client/root.cert.pem"
              :sslfactory "org.postgresql.ssl.jdbc4.LibPQFactory"})
(def zk-spec (atom {:report-api-v1 "https://192.168.1.2:8443"}))

(defmethod format-clause :create-table-if [[_ tablename] _]
  (str "CREATE TABLE IF NOT EXISTS " (-> tablename first to-sql)) )
(defhelper create-table-if [m tablename]
  (assoc m :create-table-if (collify tablename)))

(defn create-chain-auth-tabs-if [db-spec]
  (j/execute! db-spec
              (-> (create-table-if :chain_auth_client)
                  (with-columns [[:id :TEXT (sql/call :primary-key)]
                                 [:secret :TEXT]
                                 [:display :TEXT]
                                 [:emails :TEXT]
                                 [:scopes :TEXT]
                                 [:auth_types :TEXT]
                                 [:expire_period :INT]
                                 [:ms_create_ts :TEXT]
                                 [:ms_update_ts :TEXT]])
                  sql/format))
  (j/execute! db-spec
              (-> (create-table-if :chain_auth_user)
                  (with-columns [[:id :TEXT (sql/call :primary-key)]
                                 [:secret :TEXT]
                                 [:display :TEXT]
                                 [:email :TEXT]
                                 [:roles :TEXT]
                                 [:ms_create_ts :TEXT]
                                 [:ms_update_ts :TEXT]])
                  sql/format))
  (j/execute! db-spec
              (-> (create-table-if :chain_auth_access_token)
                  (with-columns [[:id :TEXT]
                                 [:client_id :TEXT]
                                 [:scopes :TEXT]
                                 [:auth_type :TEXT]
                                 [:expire_ts :TEXT]
                                 [:user_id :TEXT]
                                 [:user_roles :TEXT]
                                 [:ms_create_ts :TEXT]])
                  sql/format))
  (j/execute! db-spec
              (-> (create-table-if :chain_auth_acl)
                  (with-columns [[:id :TEXT (sql/call :primary-key)]
                                 [:hosts :TEXT]
                                 [:client_ids :TEXT]
                                 [:scopes :TEXT]
                                 [:user_ids :TEXT]
                                 [:roles :TEXT]
                                 [:ms_create_ts :TEXT]])
                  sql/format)))

(defn insert-tab [db-spec tab tuples]
  (let [update-tuple (fn [t] (merge t {:ms_create_ts (latest-ts)} (when (:secret t) {:secret (hashers/derive (:secret t))})))]
    (j/execute! db-spec (-> (insert-into tab) (values (map update-tuple tuples)) sql/format) )))
(defn select-tab [db-spec tab id] (-> (j/query db-spec (-> (select :*) (from tab) (where [:= :id id]) sql/format)) first) )
(defn insert-auth-clients [db-spec clients] (insert-tab db-spec :chain_auth_client clients) )
(defn insert-auth-users [db-spec users] (insert-tab db-spec :chain_auth_user users) )
(defn insert-auth-token [db-spec token] (insert-tab db-spec :chain_auth_access_token [token]))
(defn select-auth-client [db-spec id] (select-tab db-spec :chain_auth_client id))
(defn select-auth-user [db-spec id] (select-tab db-spec :chain_auth_user id))
(defn select-auth-token [db-spec id] (select-tab db-spec :chain_auth_access_token id))

(defn generate-json-string [edn]  (if (string? edn) edn (generate-string edn) ))
(defn not-implemented []  (generate-json-string ["not-implemented!"]))
(defn merge-chain-args [chain-args api result]
  (merge chain-args {:status (:status result) :result (merge (:result chain-args) result) api result}))



;; {:GET [:index :show] :POST [:create] :PUT [:update] :DELETE [:delete]}
(defn cache-resource-show [chain-args]
  (prn {:cache-resource-show {:chain-args chain-args}})
  (let [result {:status :forward}]
    (merge-chain-args chain-args :cache-resource-show result)))


(defn auth-token-save [db-spec {{client-id :client-id request-client-secret :client-secret body :body} :args :as chain-args}]
  (prn {:auth-token-save {:client-id client-id :client-secret request-client-secret :body body}})
  (if-not client-id
    (merge-chain-args chain-args :auth-token-save {:status :error :body "client-id not found!"})
    (let [[request-scope request-auth-type] (map body ["scope" "auth_type"])
        [client-secret client-scopes client-auths expire-period] (map (select-auth-client db-spec client-id) [:secret :scopes :auth_types :expire_period])
        result (cond (not (hashers/check request-client-secret client-secret))
                     {:status :error :body (format "client[%s] auth failed!" client-id)}
                     (some (partial not= request-scope) (clojure.string/split (or client-scopes "") #","))
                     {:status :error :body (format "invalid scope: %s" request-scope)}
                     (some (partial not= request-auth-type) (clojure.string/split (or client-auths "") #","))
                     {:status :error :body (format "invalid auth_type: %s" request-auth-type)}
                     (= request-auth-type "password")
                     (let [[user-id request-user-secret] (map body ["username" "password"])
                           [user-secret user-roles] (map (select-auth-user db-spec user-id) [:secret :roles])]
                       (if (hashers/check request-user-secret user-secret)
                         (doto {:id (str (java.util.UUID/randomUUID))
                                :client_id client-id :scopes request-scope :auth_type request-auth-type
                                :expire_ts (future-ts expire-period)
                                :user_id user-id :user_roles user-roles}
                           (#(insert-auth-token db-spec %)))
                         {:status :error :body (format "user[%s] auth failed!" user-id)})))]
    (merge-chain-args chain-args :auth-token-save result)))   )

(defn auth-token-show [db-spec {{token-id :token-id} :args :as chain-args}]
  (prn {:auth-token-show {:token-id token-id}})
  (let [token-info (select-auth-token db-spec token-id)
        result (if (pos? (compare (latest-ts) (:expire_ts token-info) ))
                 {:status :error :body (format "token[%s] expired!" token-id)}
                 token-info)]
    (merge-chain-args chain-args :auth-token-show result)) )

(defn auth-acl-show [db-spec chain-args]
  (prn {:auth-acl-show {:chain-args chain-args}})
  (let [result {:status :ok}]
    (merge-chain-args chain-args :auth-acl-show result))  )


(defn dispatch-route-save [zk-spec {{{id "id" url "url"} :body} :args :as chain-args}]
  (prn {:dispatch-route-save {:id id :url url}})
  (let [result {:status :success :route (swap! zk-spec assoc id url)}]
    (merge-chain-args chain-args :dispatch-route-save result)) )

(defn dispatch-route-index [zk-spec chain-args]
  (prn {:dispatch-resource-index {:zk-spec @zk-spec :chain-args chain-args}})
  (let [result {:status :success :routes @zk-spec}]
    (merge-chain-args chain-args :dispatch-route-index result))  )

(defn dispatch-resource-show [zk-spec {{token-id :token-id request-method :request-method
                                        resource-id :resource-id query-params :query-params body :body} :args :as chain-args}]
  (prn {:dispatch-resource-show {:token-id token-id :zk-spec @zk-spec :resource-id resource-id :query-params query-params :body body}})
  (let [route  (-> resource-id (clojure.string/split #":") first keyword)
        path (-> resource-id (clojure.string/replace #":|=" "/"))
        request {:url (str (@zk-spec route) "/" path)
                 :method request-method
                 :headers {"Authorization" (format "Bearer %s" token-id)}
                 :query-params query-params
                 :body (generate-string body)
                 :as :text
                 :timeout 2000
                 :insecure? true}
        _ (prn {:request request})        
        response  @(httpkit-client/request request)]
    (let [{:keys [error body]} response]
      (if error {:status :error :body (prn-str error)} body))    ) )

(defn chain-resource-show [{:keys [token-spec acl-spec route-spec]} args]
  (reduce #(if-not (contains? #{:success :error} (:status %1)) (%2 %1) (reduced %1))
          (merge (array-map :status :init) args)
          [cache-resource-show
           (partial auth-token-show token-spec)
           (partial auth-acl-show acl-spec)
           (partial dispatch-resource-show route-spec)]))


(defn parse-request [request]
  (let [args (select-keys request [:remote-addr :headers :body :query-params :request-method])
        header-ext (when-let [authorization-header (get-in args [:headers "authorization"])]
                        (let [[authorization-type authorization-string] (clojure.string/split authorization-header #" ")]
                          (cond (= authorization-type "Basic")
                                (zipmap [:client-id :client-secret]
                                        (-> (->> authorization-string .getBytes base64/decode (map char) (apply str))
                                            (clojure.string/split #":")))
                                (= authorization-type "Bearer") {:token-id authorization-string}) ))
        body-realize (when-let [body (:body args)] {:body (->> body .bytes (new String) parse-string)})]
    (merge args header-ext body-realize)))

(defroutes handler
  (context "/chain-api-v1" []
           (GET "/ping" [] (generate-json-string ["pong"]))
           (POST "/token" request
                 (->> {:args (-> request parse-request) }
                      (auth-token-save pg-spec)
                      :auth-token-save
                      generate-json-string) )
           (GET "/token/:token-id" [token-id :as request]
                (->> {:args (-> request parse-request (assoc :token-id token-id)) }
                     (auth-token-show pg-spec)
                     :auth-token-show
                     generate-json-string))
           (ANY "/resource/:resource-id" [resource-id :as request]
                (->> {:args (-> request parse-request (assoc :resource-id resource-id))}
                     (chain-resource-show {:token-spec pg-spec :route-spec zk-spec})
                     generate-json-string))
           (POST "/route" request
                 (->> {:args (-> request parse-request)}
                      (dispatch-route-save zk-spec)
                      :dispatch-route-save
                      generate-json-string) )
           (GET "/route" request 
                (->> {:args (-> request parse-request) }
                     (dispatch-route-index zk-spec) :dispatch-route-index generate-json-string))
           ))

(defrecord HttpServer [http-config]
  component/Lifecycle
  (start [{:keys [port] :as component}]
    (prn "Starting HTTP Server")
    (let [server (httpkit-server/run-server (-> handler
                                                (wrap-defaults (assoc-in ring.middleware.defaults/site-defaults [:security :anti-forgery] false))
                                                (wrap-cors :access-control-allow-origin [#".*"]
                                                           :access-control-allow-methods [:get :put :post :delete]))
                                            {:port port})]
      (prn (format "Http-Kit server is running at http://0.0.0.0:%s" port))
      (assoc component :server server)))
  (stop [{:keys [server] :as component}]
    (try+ (server :timeout 5000) (finally (prn "Game is over!")))
    (assoc component :server nil)
    (prn "Stopping HTTP Server")))

(defn get-system [{:keys [http-config]}]
  (-> (component/system-map
       :http (map->HttpServer http-config))
      (component/system-using {})) )

(def system nil)
(defn init [] (alter-var-root #'system (constantly (get-system {:http-config {:port 1111}}) )))
(defn start [] (alter-var-root #'system component/start))
(defn stop [] (alter-var-root #'system (fn [s] (when s (component/stop s)))))
(defn go [] (init) (start))
(defn reset [] (stop) (refresh :after 'user/go))

(defn -main [] (go) (<!! (chan)))

(comment
  (def clients [{:id "cocacola-api-v1" :secret "spiderdt.com" :emails "larluo@spiderdt.com"
                 :scopes "REPORT-API-V1" :auth_types "password" :expire_period (* 60 60 12)}
                {:id "jupiter-api-v1" :secret "spiderdt.com" :emails "larluo@spiderdt.com"
                 :scopes "STORAGE-API-V1,SCHEDULER-API-V1,DB-API-V1,REPORT-API-V1" :auth_types "password" :expire_period (* 60 60 12)}])
  (def users [{:id "larluo@spiderdt.com" :secret "spiderdt.com" :email "larluo@spiderdt.com" :roles "ADMIN"}
              {:id "chong@spiderdt.com" :secret "spiderdt.com" :email "chong@spiderdt.com" :roles "GUEST"}])
  (create-chain-auth-tabs-if pg-spec)
  (insert-auth-clients pg-spec clients)
  (insert-auth-users pg-spec users)
  (select-auth-client pg-spec "jupiter-api-v1")
  (select-auth-user pg-spec "larluo@spiderdt.com")
  
  (auth-token-save pg-spec {:args {:client-id "cocacola-api-v1"
                                   :body {"scope" "REPORT-API-V1" "auth_type" "password" "username" "larluo@spiderdt.com" "password" "spiderdt.com"}}} )

  (select-auth-token pg-spec "cfa18b9e-f89e-4139-9014-246095e0f0b1")
  (go)
  (stop)
  
  )
