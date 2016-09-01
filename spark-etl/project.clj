(defproject spark-etl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.rpl/specter "0.11.2"]
                 [org.clojure/data.csv "0.1.3"]
                 [org.scala-lang/scala-library "2.11.8"]
                 [yieldbot/flambo "0.8.0-SNAPSHOT"]
                 [org.apache.thrift/libthrift "0.9.3"]
                 [com.taoensso/timbre "4.7.0"]
                 [com.google.guava/guava "16.0"]
                 [org.apache.hbase/hbase-thrift "1.2.2"]
                 #_[org.apache.hbase/hbase-spark "1.2.2"]
                 [org.apache.hbase/hbase-client "1.2.2"]
                 [org.apache.hbase/hbase-server "1.2.2"]
                 [org.apache.hbase/hbase-protocol "1.2.2"]
                 [org.apache.hbase/hbase-common "1.2.2"]
                 [incanter "1.5.7"]
                 [clj-time "0.11.0"]
                 [rm-hull/infix "0.2.9"]])
