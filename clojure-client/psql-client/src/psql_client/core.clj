(ns psql-client.core
  (:require [clojure.java.jdbc :as j]))

(def pg-info {:dbtype "postgresql"
              :dbname "ms"
              :host "192.168.1.2"
              :user "ms"
              :password "spiderdt"
              :sslmode "require"
              :sslkey "/data/ssl/client/client.key.pk8"
              :sslcert "/data/ssl/client/client.cert.pem"
              :sslrootcert "/data/ssl/client/root.cert.pem"
              :sslfactory "org.postgresql.ssl.jdbc4.LibPQFactory"
              })

(j/query pg-info
         ["select * from report"]
         )

