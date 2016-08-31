(ns log2kafka.core
  (:require [clojure.java.io :as io :refer [file resource writer reader]]
            [clojure.string :as string :refer [trim]]
            [clojure.core.async :refer [chan >!! <!! close! timeout alts!! sliding-buffer]]
            [aero.core :refer [read-config]]
            [clj-time.core :as t]
            [clj-time.local :as l :refer [format-local-time]]
            [clj-time.periodic :as p]
            [clojure.template :refer [apply-template]]
            [me.raynes.conch.low-level :as sh]
            [taoensso.timbre :refer [info debug warn]]
            [clj-time.format :as f]
            [clj-kafka.admin :as admin]
            [clj-kafka.zk :refer [brokers topics]]
            [clj-kafka.new.producer :as zkp :refer [producer byte-array-serializer record]])
  (:import [java.io RandomAccessFile]))

(defn commit-fileoffset [filename offset]
  (info "commit-fileoffset" filename offset)
  (io/copy (str offset "\n") (file (str "resources/" filename ".bk"))))

(defn read-fileoffset [filename]
  (let [bk-res (-> filename (str ".bk") resource)]
    (if bk-res (-> bk-res slurp trim Integer/parseInt) 0)))

(defn parse-file-pattern [file-pattern ts-vector]
  (apply-template '[year month day hour minute]
                  (map #(if (keyword? %) (-> % name symbol) %) file-pattern)
                  ts-vector))

(defn topic-name [[_ & file-pattern]]
  (->> file-pattern (map (partial name)) string/join (str "log-")))

(defn log-reading [[zk-servers topic] file-pattern-with-ts]
  (let [filepath (string/join file-pattern-with-ts)
        _ (info "processing file:" filepath)
        filename (-> file-pattern-with-ts rest string/join)
        pending-offset (atom (read-fileoffset filename))
        tail-proc (sh/proc "tail" "-F" "-c" (str "+" @pending-offset) filepath)
        tail-ch (chan 307200)
        _ (future (doall (for [line (-> tail-proc :out reader line-seq)]
                           (>!! tail-ch line))))
        commit-interval 2000
        commit-offset (future (loop [] (Thread/sleep commit-interval)
                                    (commit-fileoffset filename @pending-offset)
                                    (recur)))
        empty-read-back-off 500]
    (with-open [p (producer {"bootstrap.servers" (->> zk-servers (map #(str % ":9092")) (string/join ","))}
                            (byte-array-serializer) (byte-array-serializer))]
      (loop []
        (let [result (first (alts!! [tail-ch (timeout empty-read-back-off)]))]
          (if result
            (do #_ (debug (str "result:[" result "]"))
                (zkp/send p (record topic (.getBytes result)) (fn [_ e] (when-not e)))
                (swap! pending-offset + 1 (count result))
                (recur))
            (do (sh/destroy tail-proc)
                (future-cancel commit-offset)
                (str "resources/" filename ".bk"))))))))

(defn log-reading-roll [{:keys [zk-servers file-pattern start-ts end-ts period]}]
  (info "check and create partition:" (topic-name file-pattern))
  (with-open [zk (admin/zk-client (string/join "," zk-servers))]
    (if-not (admin/topic-exists? zk (topic-name file-pattern))
      (admin/create-topic zk (topic-name file-pattern) {:replication-factor 1})))
  (doseq [ts (p/periodic-seq
              (if start-ts (l/to-local-date-time start-ts) (l/local-now))
              (l/to-local-date-time (or end-ts "9999-99-99T00:00:00"))
              period)]
    (->> (format-local-time ts :date-hour-minute-second)
         (re-find #"(\d*)-(\d*)-(\d*)T(\d*):(\d*):(\d*)")
         rest
         (parse-file-pattern file-pattern)
         (log-reading [zk-servers (topic-name file-pattern)]))))

(defn -main [filepath]
  (log-reading-roll (-> filepath read-config eval)
                    #_config-edn-test))

(comment
  (def config-edn-test {:file-pattern ["/data/larluo/var/"
                                       "BookBase.TLogExplanationServer_qd_readtime_" :year :month :day :hour ".log"]
                        :start-ts "2016-06-01T00:00:00"
                        :period (clj-time.core/hours 1)})
    (topics {"zookeeper.connect" "10.205.3.23:2181"}))
