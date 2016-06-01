(defproject storm-trident-drpc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.7.0"]
                                       [org.apache.storm/storm-core "1.0.0"]]}}
  :dependencies [[slingshot "0.12.2"]
                 [yieldbot/marceline "0.3.1-SNAPSHOT"]])
