(defproject storm-trident-kafka-redis "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.7.0"]
                                       [org.apache.storm/storm-core "1.0.0"]]}}
  :dependencies [[slingshot "0.12.2"]
                 [clj-kafka "0.3.4" :exclusions [org.slf4j/slf4j-log4j12 log4j/log4j]]
                 [clj-time "0.11.0"]
                 [com.rpl/specter "0.10.0"]
                 [com.taoensso/timbre "4.3.1"]
                 [com.taoensso/carmine "2.12.2" :exclusions [com.taoensso/encore]]
                 [yieldbot/marceline "0.3.1-SNAPSHOT"]
                 [org.apache.storm/storm-redis "1.0.0"]
                 [org.apache.storm/storm-kafka "1.0.0"]])
