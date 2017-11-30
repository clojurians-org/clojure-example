(ns jvm-heapdump.core)

(do
  (import '[java.io FileInputStream BuferedInputStream DataInputStream EOFException])
  )

(def tag-kv {0x01 :string-in-utf-8
             0x02 :load-class
             0x03 :unload-class
             0x04 :stack-frame
             0x05 :alloc-trace
             0x06 :alloc-sites
             0x07 :heap-summary
             0x0a :start-thread
             0x0b :end-thread
             0x0c :heap-dump
             0x1c :heap-dump-segment
             0x2c :heap-dump-end
             0x0d :cpu-samples
             0x0e :control-settings})

(defn parse-header [in]
  (->> in
    ((juxt (fn [in]
               (->> #(.readByte in) repeatedly
                    (take-while (partial not= 0))
                    byte-array (new String) ))
           #(.readInt %)
           #(.readLong %)))
    (zipmap [:format :id-size :start-ts]))
  )

(defn parse-record-prefix [in]
  (->> in
    ((juxt #(.readByte %)
           #(.readInt %)
           #(.readInt %)))
    (zipmap [:tag :ts :bytes-left]))
  )

(defmulti parse-tag (fn [in header {tag :tag :as prefix}] (tag-kv tag)))

(defmethod parse-tag :heap-dump-segment [in header {:keys [tag bytes-left] :as prefix}]
  (println (format "[parse-record] type: %s, tag: %s, bytes-left: %s" (tag-kv tag) tag bytes-left))
  (dorun (repeatedly bytes-left #(.readByte in)))
  )

(defn parse-record [in header]
  (try (parse-tag in header (parse-record-prefix in))
    (catch EOFException e (println "[parse-record] EOF!") :eof)))

(defn -main [file-path]
  (println (format "[main.start] begin to parse file: %s" file-path))
  (with-open [hprof-in (-> file-path FileInputStream. BufferedInputStream. DataInputStream.)]
    (let [header (parse-header hprof-in)]
      (some :eof (repeatedly #(parse-record hprof-in header)))) )
  (println (format "[main.end] parse finished!"))
  )

(comment
  (-main "/path/larluo.bin")
  )
