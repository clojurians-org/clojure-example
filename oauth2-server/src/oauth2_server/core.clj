(ns oauth2-server.core
  (:require [clauth.client :refer [client-store clients register-client reset-client-store!]]
            [clauth.user :refer [user-store users register-user reset-user-store!]]
            [clauth.token :refer [token-store]]
            [clauth.auth-code :refer [auth-code-store]]
            [clauth.endpoints :refer [token-handler]]
            [clauth.store.redis :refer [create-redis-store]]
            [ring.middleware.cookies :refer [wrap-cookies]]
            [ring.middleware.session :refer [wrap-session]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.adapter.jetty :refer [run-jetty]])
  (:gen-class))

(defn handler [request]
  (case (request :uri)
    "/token" ((token-handler) request)
    ))

(defn -main []
  (println " curl http://192.168.1.2:3000/token -d grant_type=client_credentials -u [client-id]:[client-secret]...")
  (reset! client-store (create-redis-store "clients"))
  (reset! token-store (create-redis-store "tokens"))
  (reset! auth-code-store (create-redis-store "auth-codes"))
  (reset! user-store (create-redis-store "users"))
  (run-jetty (-> handler
                 (wrap-keyword-params)
                 (wrap-params)
                 (wrap-cookies)
                 (wrap-session)
                 )
             {:port 3000}))

(comment
  (reset! client-store (create-redis-store "clients"))
  (reset! token-store (create-redis-store "tokens"))
  (reset! auth-code-store (create-redis-store "auth-codes"))
  (reset! user-store (create-redis-store "users"))
  (clients)
  (register-client "spiderdt" "www.spiderdt.com")
  (users)
  (register-user "spiderdt" "spiderdt")
  (reset-client-store!)
  (reset-user-store!)
  )
