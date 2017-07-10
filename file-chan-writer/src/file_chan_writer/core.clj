(ns file-chan-writer.core)

(require '[net.cgrand.xforms :as x])
(require '[clojure.core.async :refer [chan >!! <!! close! timeout alts!! >! <! go]]) 
(require '[clojure.java.io :as io])
(require '[clojure.tools.logging :as log])
(require '[me.raynes.fs :as fs])

(def chan-size 307200)
(defn open-file-chan
  ([filepath] (open-file-chan filepath (* 50 1000)))
  ([filepath ms]
    (let [file-chan (chan chan-size)
          file-out (io/writer (str filepath ".writing") :append true)]
            (go (try (loop []
                     (if-let [result (first (alts!! [file-chan (timeout ms)]))]
                       (do (.write file-out (str result "\n")) (.flush file-out) (recur))
                       (do (log/info {:object filepath :event :timeout})
                           (.flush file-out) (.close file-out) (close! file-chan)
                           (fs/rename (str filepath ".writing") filepath))))
                  (catch Exception e (prn e)) ))
      file-chan)))

(def ch1 (open-file-chan "abc"))

(>!! ch1 "hello world xyz")

(def ch2 (open-file-chan "abc"))
(>!! ch2 "why2why2why3 66")

(when ch2 "tre")
