(ns excel-parser.core
  (:require [incanter.excel :refer [read-xls]]
            [incanter.core :as i]
            [incanter.io :as iio]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.core.async :refer [chan >!! <!! close! timeout alts!! sliding-buffer go >! <!]])
  (:import  [org.apache.poi.openxml4j.opc OPCPackage]
            [org.apache.poi.xssf.eventusermodel XSSFReader]
            [javax.xml.parsers SAXParserFactory]
            [org.apache.poi.xssf.usermodel XSSFRichTextString XSSFWorkbook]
            [org.xml.sax InputSource]
            [java.io ByteArrayOutputStream]))

(defn excel->reader [in-file-path]
  (->> in-file-path OPCPackage/open (new XSSFReader)))

(defn excel->string-table [reader]
  (into {} (map-indexed #(vector %1 (->> %2 (new XSSFRichTextString) .getString)) (-> reader .getSharedStringsTable .getItems))))

(defn excel->style-table [reader]
  (let [style-table (-> reader .getStylesTable)]
    (into {} (map #(vector % (->> % (.getStyleAt style-table) .getDataFormatString)) (range (.getNumCellStyles style-table))))))

(defn excel->sheet-kv [reader]
  (let [sheets-data (.getSheetsData reader)]
    (->> (repeatedly (fn [] (when (.hasNext sheets-data) (vector (.next sheets-data) (.getSheetName sheets-data)))))
         (take-while some?) (map (comp vec reverse)) (into {}))))

(defn excel->first-sheet [reader]
  (-> reader excel->sheet-kv first second))

#_ (let [x 2 y 2]
     (cond-> []
       (odd? x) (conj "x is odd")
       (zero? (rem y 3)) (conj "y is divisible by 3")
       (even? y) (conj "y is even")))

(defn sheet->channel [in-sheet-stream string-table style-table a-chan]
  (with-open [in-stream in-sheet-stream]
    (let [[cur-cell cur-row acc-rows] [(atom {}) (atom {}) (atom [])]
          batch-size 10000
          start-element-fn (fn [edn] (when (= "c" (:name edn)) (reset! cur-cell {:attributes (:attributes edn)})))
          end-element-fn (fn [edn] (cond (= "worksheet" (:name edn)) (do  (>!! a-chan @acc-rows) (>!! a-chan :done))
                                         (= "row" (:name edn)) (do (swap! acc-rows conj @cur-row) (reset! cur-row {})
                                                                   (when (>= (count @acc-rows) batch-size)
                                                                     (>!! a-chan @acc-rows) (reset! acc-rows [])))))
          characters-fn (fn [edn] (swap! cur-row assoc (-> @cur-cell (get-in [:attributes "r"]) (clojure.string/split #"\d+") first)
                                         (cond-> (:text edn)
                                           (and (not= (get-in @cur-cell [:attributes "t"]) "s") (some->> (get-in @cur-cell [:attributes "s"]) Integer/parseInt style-table (re-find #".*yy.*")))
                                           (#(.formatRawCellContents (new DataFormatter) (Double/parseDouble %) 0 "yyyy-MM-dd hh:mm:ss"))
                                           (= (get-in @cur-cell [:attributes "t"]) "s")
                                           (#(->> % Integer/parseInt string-table)))))
          attr->kv (fn [attr] (into {} (map #(vector (.getName attr %) (.getValue attr %)) (range (.getLength attr)))))]
      (doto (-> (SAXParserFactory/newInstance) .newSAXParser .getXMLReader)
        (.setContentHandler (proxy [org.xml.sax.helpers.DefaultHandler] []
                              (startElement [uri localName name attributes] (start-element-fn (zipmap [:name :attributes :fn] [name (attr->kv attributes) :startElement])))
                              (endElement [uri localName name] (end-element-fn (zipmap [:name :fn] [name :endElement])))
                              (characters [ch start length] (characters-fn (zipmap [:text :fn] [(String. ch start length)  :characters])))))
        (.parse (InputSource. in-stream))))))

(defn sheet->event-seq [in-sheet-stream string-table style-table]
  (let [params-chan (chan 5)]
    (future (try (sheet->channel in-sheet-stream string-table style-table params-chan) (catch Exception _ (>!! params-chan :error))))
    (doto (->> (repeatedly (fn [] (<!! params-chan)))
               (take-while #((complement contains?) #{:done :error} %)))
      (fn [_] (close! params-chan)))))

(defn write-first-sheet [in-file-path]
  (with-open [out-file (io/writer (str in-file-path ".csv"))]
    (let [reader (excel->reader in-file-path)
          column-count (atom nil)
          columns-name (->> (combo/cartesian-product " ABCDEFGHIJKLMNOPQRSTUVWXYZ" "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
                            (map (comp clojure.string/trim clojure.string/join)))
          column-take (fn [column-count row] (map row (take column-count columns-name)))]
      (doseq [batch-rows (sheet->event-seq (excel->first-sheet reader) (excel->string-table reader) (excel->style-table reader))]
        (when-not @column-count (reset! column-count (count (first batch-rows))))
        (csv/write-csv out-file (map (partial column-take @column-count) batch-rows))))))

(comment

  (time
   (write-first-sheet "/home/spiderdt/work/git/spiderdt-team/var/data/stg_bk.db/d_tutuanna_order/2017-02-22T09_31_31/16年10月上.xlsx")))

(defn -main [& paths]
  (println "[excel2csv]==== [convert file] ====")
  (mapv println paths)
  (println "[excel2csv]================")
  (doall (pmap write-first-sheet paths))
  (println "[excel2csv]==== [done] ===")
  #_ (doseq [path paths]
       (prn {:current-file path})
       (-> path (read-xls :header true) (i/save (str path ".csv")))))
