(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
   "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject my-kafka-streams "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["confluent" "http://packages.confluent.io/maven/"]
                 ["confluent-snapshots" "https://s3-us-west-2.amazonaws.com/confluent-snapshots/"]]
  :plugin-repositories [["confluent" "http://packages.confluent.io/maven/"]
                        ["confluent-snapshots" "https://s3-us-west-2.amazonaws.com/confluent-snapshots/"]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.kafka/kafka-streams "2.0.0-cp1" ]
                 [cheshire "4.0.3"]])
