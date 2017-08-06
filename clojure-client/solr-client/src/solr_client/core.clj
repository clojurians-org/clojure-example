(ns solr-client.core)
(do
  (require '[org.httpkit.client :as http])
  (require '[byte-streams :refer [compare-bytes print-bytes]])
  (import '[java.io PipedInputStream PipedOutputStream])
  (import '[org.apache.solr.common.util JavaBinCodec])
  (import '[org.apache.solr.common SolrInputDocument])
  (import '[java.io PipedInputStream PipedOutputStream])

  (import '[org.apache.lucene.index IndexWriter])
  (import '[org.apache.lucene.store Directory FSDirectory])
  (import '[java.nio.file FileSystem FileSystems Path Paths])

  (import '[org.apache.lucene.analysis.standard StandardAnalyzer])
  (import '[org.apache.lucene.index IndexWriterConfig])
  (import '[org.apache.lucene.document Document Field Field$Store Field$Index Field$TermVector] )
  )


(import '[org.apache.lucene.store NIOFSDirectory])
(def directory (new NIOFSDirectory (Paths/get (format "/tmp/%s" uuid) (into-array String [])) )) 
(def writer (new IndexWriter directory (new IndexWriterConfig (new StandardAnalyzer)) )) 

(def solr-path "/Users/larluo/work/git/solr-clj/solr-5.5.1/solr/example/cloud/node1/solr/gettingstarted_shard2_replica1/data/index")
(def solr-directory (new NIOFSDirectory (Paths/get solr-path (into-array String []))))
(def solr-writer (new IndexWriter solr-directory (new IndexWriterConfig (new StandardAnalyzer)) ))

(.numDocs solr-writer)
#_(.deleteAll solr-writer)
(.addIndexes solr-writer (into-array Directory [directory]))
(.close solr-writer)


(defn str! [s] (if (keyword? s) (name s) (str s)))
(defn lucene-doc [m]
  (reduce #(do (.add %1 (new Field (-> %2 first str!) (second %2) Field$Store/YES Field$Index/NOT_ANALYZED_NO_NORMS Field$TermVector/YES)) %1) (new Document) m))
(defn lucene-docs [ms] (map lucene-doc ms))
(.addDocuments writer (lucene-docs [{:id "cc" :name "cc"} {:id "dd" :name "dd"}])) 

(.numDocs writer)
(.close writer)


(defn flip-stream [f] (let [in (new PipedInputStream)] (with-open [out (new PipedOutputStream in)] (f out)) in))
(def java-bin-codec (new JavaBinCodec))
(defn build-doc [doc] (reduce #(do (.addField %1 (-> %2 first name) (second %2)) %1) (new SolrInputDocument) doc) )

(defmulti solr-bin-codec (fn [obj] obj))

(defmethod solr-bin-codec )
(defmethod solr-bin-codec java.util.Map [obj] "a")
(defmethod solr-bin-codec java.util.List [obj] "b")


(solr-bin-codec {:a 11})
(solr-bin-codec ["a" "b" "c"])
(type {:a 11})
(compare-bytes
  (flip-stream #(.marshal java-bin-codec (build-doc {:a 11}) %))
  (flip-stream #(.marshal java-bin-codec (build-doc {:a 11}) %))
  )



(->> @(http/post "http://localhost:8983/solr/collection/update"
                 {:as :stream
                  :query-params {:wt "javabin" "update.contentType" "application/javabin"}
                  :body (flip-stream (fn [it] (.marshal java-bin-codec (reduce #(do (.addFields))))))}))

(.unmarshal java-bin-codec
  )


