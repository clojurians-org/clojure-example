(require '[clj-http.client :as client])
(require '[clojure.string :as str])
(require '[clj-http.cookies :as cookies])
(require '[clojure.data.json :as json])
(require '[clojure.data.csv :as csv])
(require '[clojure.java.io :as io])
(import '(org.joda.time DateTime Period))
(import '(org.joda.time.format DateTimeFormat))

#_(doto (System/getProperties) (.put "proxySet" "true") (.put "proxyHost" "***") (.put "proxyPort" "80"))

(defn parse-date [date-str]
  (-> (DateTimeFormat/forPattern "yyyy-MM-dd") (.parseDateTime date-str)))
(defn unparse-date [date]
  (-> (DateTimeFormat/forPattern "yyyy-MM-dd") (.print date)) )
(defn add-days[date-str days]
  (when date-str (-> date-str parse-date (.plus (Period. 0 0 0 days 0 0 0 0)) unparse-date)) )
(defn date-seq [start-dt end-dt & {:keys [year month day]
                                   :or {year 0 month 0 day 0}}]
  (let [period (Period. year month 0 day 0 0 0 0)]
    (if (zero? (.getDays period))
      [start-dt end-dt]
      (concat (take-while
               #(and ((complement zero?) (.getDays period)) (neg? (.compareTo % end-dt)))
               (iterate #(-> % (.plus period)) start-dt) ) [end-dt]) ) ) )
(defn date-seq-rng [start-dt end-dt & rest]
  (map (fn [[first second]]
         [first (-> second (.plus (Period. 0 0 0 -1 0 0 0 0)))] )
       (partition 2 1 (apply date-seq (flatten [start-dt (.plus end-dt (Period. 0 0 0 1 0 0 0 0)) rest]))) ) )
(defn date-seq-str [start-dt-str end-dt-str & rest]
  (map #(unparse-date %) (apply date-seq (flatten [(parse-date start-dt-str) (parse-date end-dt-str) rest]))) )
(defmacro date-seq-rng-str [start-dt-str end-dt-str & rest]
  `(map #(map unparse-date %) (date-seq-rng (parse-date ~start-dt-str) (parse-date ~end-dt-str) ~@rest)) )
(defn between? [curDt startDt endDt]
  (and (<= (compare startDt curDt) 0) (<= (compare curDt endDt)  0)) )

(letfn [(get-arr [col ks] (cond (sequential? col) (map  ks col) (map? col) [(get col ks)] ) )]
  (defn get-in-arr [coll paths]
    (reduce  (fn [cur-coll path] (mapcat #(get-arr % path) cur-coll))  coll paths) ))
(defn csv-map-get [csv-matrix] (let [[header & data] csv-matrix
                                     header-keyword (map keyword header)]
                                 (map #(zipmap header-keyword %) data) ))

(defn select-values [amap ks] (reduce #(conj %1 (cond (keyword? %2) (amap %2) :else %2)) [] ks))
(defn matrix-get [db-result cols] (map #(select-values % cols) db-result))

(defn google-token-auth [{:keys [client_id client_secret refresh_token]}]
    (let [response (client/post "https://www.googleapis.com/oauth2/v3/token"
                                {:as :json
                                  :form-params {:grant_type "refresh_token"
                                                :client_id client_id
                                                :client_secret client_secret
                                                :refresh_token refresh_token}} )]
        {:Authorization (-> response :body :access_token (#(str "Bearer " %)))} ) )

(defn google-file-list [{:keys [account-id app-id] :as conf}]
    (let [response (client/get (str "https://www.googleapis.com/storage/v1/b/pubsite_prod_rev_" account-id "/o")
                           {:as :json
                            :query-params {:prefix (str "stats/installs/installs_" app-id "_")}
                            :headers (google-token-auth conf) }) ]
      (-> response :body (cons []) (get-in-arr [:items]) first
           (#(filter (fn [file-info] (-> file-info :name (.endsWith "_overview.csv"))) %))
           (matrix-get [:name :mediaLink :updated]) )))

(defn google-file-download [{:keys [startDt endDt filename]}
                            {:keys [account-id app-id out-columns] :as conf}]
  (let [file-list (->> (google-file-list conf)
                      (map (fn [[filepath :as file-info]]
                               (-> app-id (str "_(\\d{6})_overview.csv") re-pattern (re-find filepath) (nth 1)
                               (str/replace #"(\d{4})(\d{2})" "$1-$2-01") (cons file-info) )))
                      (filter (fn [[file-dt]]
                                (between? file-dt startDt endDt) )))]
    (->> file-list
         (map (fn [[file-dt filepath mediaLink]]
                (let [response  (client/get mediaLink {:as :stream :headers (google-token-auth conf)
                                                       :retry-handler (fn [ex try-count http-context]
                                                                        (println "Got:" ex)
                                                                        (if (> try-count 4) false true)) })
                      dummy (println " [OK] REPORT DATA RETURN SUCCESSFULLY!")
                      filename-prefix (str filename ".dir/" file-dt) ]
                  (println " [*] BUFFER CSV DATA TO LOCAL DISK: " (str filename-prefix ".csv.buffer"))
                  (with-open [in-data (:body response)] (io/copy in-data (io/file (str filename-prefix ".csv.buffer"))))
                  (with-open [in-csv (io/reader (str filename-prefix ".csv.buffer") :encoding "UTF-16LE")
                              out-xsv (io/writer (str filename-prefix ".csv.par"))]
                    (let [matrix (-> in-csv csv/read-csv csv-map-get (matrix-get out-columns))]
                      (doseq [row matrix] (->> (concat row ["\n"]) (clojure.string/join "\007") (.write out-xsv)))
                      (println (str " [OK] CSV PARSED FINSIHED!@" (count matrix))) ))) file-dt))
     doall)))

(defn google-data-feed [{dt-range       :dt-range
                         next-dt-range  :next-dt-range
                         {:keys [offset-dt by-day]} :dt-params
                         filename :filename
                         :as command-line-map}
                        conf]
  (let [[start-dt end-dt]  (or next-dt-range dt-range)
        dummy (println (str " [INFO] data-feed start" (or next-dt-range dt-range) " -> " filename))
        dts (->> (date-seq-rng-str start-dt (or end-dt (unparse-date (DateTime.))) :day by-day)
               (mapcat (fn [[each-start-dt each-end-dt]]
                         (println "EACH " each-start-dt each-end-dt)
                         (google-file-download (merge command-line-map {:startDt each-start-dt :endDt each-end-dt}) conf) )))
        max-dt (-> dts sort last (or (add-days start-dt -1)) (add-days (+ 1 offset-dt)) ) ]
        {:dt-range [start-dt end-dt] :next-dt-range [max-dt nil] :dt-params {:offset-dt offset-dt :by-day by-day}} ))

(doto
    (google-data-feed #_{:dt-range       ["2015-08-01" nil]
                     :next-dt-range  ["2015-08-01" "2015-09-01"]
                     :dt-params      {:offset-dt -1 :by-day 30}
                     :filename       "data_feed.csv"}
                  (-> (read-string (nth *command-line-args* 0))
                      (or (clojure.main/load-script "date.conf"))
                      (merge {:filename (nth *command-line-args* 1)}) )
                  (clojure.main/load-script "application.conf")
                  #_{:client_id     "****"
                   :client_secret "****"
                   :refresh_token "****"
                   :account-id    "****"
                   :account-name  "****"
                   :app-id        "****"
                   :out-columns   ["AAAA" "Mobile Development ***"  "**.***"
                                   :Date (keyword "Package Name") (keyword "Current Device Installs") (keyword "Daily Device Installs")
                                   (keyword "Daily Device Uninstalls") (keyword "Daily Device Upgrades") (keyword "Current User Installs") (keyword "Total User Installs")
                                   (keyword "Daily User Installs") (keyword "Daily User Uninstalls")] })
    (println "@dt-result")
    (-> (str "\n") (io/copy (io/file "date.conf.next"))) )
