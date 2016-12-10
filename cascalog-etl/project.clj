(defproject cascalog-etl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-client "2.7.3"]]}}
  :repositories {"sonatype-oss-public" "http://conjars.org/repo/"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cascading/cascading-jdbc-mysql "2.5.5"]
                 [dk.ative/docjure "1.11.0"]
                 [com.taoensso/timbre "4.7.4"]
                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [mysql/mysql-connector-java "6.0.3"]
                 [honeysql "0.8.1"]
                 [org.clojure/data.json "0.2.6"]
                 [cascalog/cascalog-core "3.0.0"]
                 [clj-time "0.12.2"]])
