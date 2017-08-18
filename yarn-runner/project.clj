(defproject yarn-runner "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-common "2.8.0"]
                                       [org.apache.hadoop/hadoop-hdfs "2.8.0"]
                                       [org.apache.hadoop/hadoop-yarn-api "2.8.0"]
                                       [org.apache.hadoop/hadoop-yarn-client "2.8.0"]]}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [byte-streams "0.2.3"]
                 [aleph  "0.4.3"]
                 [gloss "0.2.6"]])
