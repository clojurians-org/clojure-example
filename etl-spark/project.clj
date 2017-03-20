(defproject etl-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.11 "2.1.0"]
                                       [org.apache.spark/spark-streaming_2.11 "2.1.0"]]}}
  :aot :all
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [hcadatalab/powderkeg "0.5.0"]
                 [clj-time "0.12.2"]])
