(defproject oauth2-server "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clauth "1.0.0-rc17"]
                 [com.taoensso/carmine "2.2.0"]
                 #_[com.taoensso/carmine "2.12.2"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [ring-cors "0.1.8"]
                 [cheshire "5.6.3"]])
