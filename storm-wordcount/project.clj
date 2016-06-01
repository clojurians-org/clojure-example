(defproject storm-wordcount "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot [storm-test.core]
  :profiles {:provided {:dependencies [[org.apache.storm/storm-core "1.0.0"]
                                       [org.clojure/clojure "1.7.0"]]}}
  :dependencies [#_[org.apache.storm/storm-core "1.0.0"]])
