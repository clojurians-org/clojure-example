(defproject onyx-eleme "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-http "2.1.0"]
                 [enlive "1.1.6"]
                 [cheshire "5.5.0"]
		 [dire "0.5.4"]
                 [org.clojure/core.async "0.2.374"]
                 [org.onyxplatform/onyx "0.9.0"]
                 [org.onyxplatform/onyx-datomic "0.9.0.0"]
                 [org.onyxplatform/onyx-redis "0.9.0.0"]
                 [com.datomic/datomic-free "0.9.5173" :exclusions [joda-time]]
                 [com.taoensso/timbre "4.3.1"] ])
