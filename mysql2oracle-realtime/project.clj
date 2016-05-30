(defproject mysql2oracle-realtime "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/java.data "0.1.1"]
                 [java-jdbc/dsl "0.1.3"]
                 [cheshire "5.6.1"]
                 [clj-time "0.11.0"]
                 [org.clojure/java.jdbc "0.6.0-alpha1"]
                 [com.oracle/ojdbc6 "11.2.0"]
                 [com.github.shyiko/mysql-binlog-connector-java "0.3.1"]])
