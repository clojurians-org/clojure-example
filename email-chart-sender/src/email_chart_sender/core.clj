(require '[postal.core :refer [send-message]])
(require '[me.raynes.conch.low-level :as sh])
(require '[hiccup.core :refer [html]])
(require '[clojure.java.io :as io])
(require '[ring.util.codec :refer [base64-encode form-encode]])

(defn slurp-bytes [x]
  (with-open [out (new java.io.ByteArrayOutputStream)]
    (clojure.java.io/copy (clojure.java.io/input-stream x) out)
    (.toByteArray out)))

(def chart-edn {"01:00" {"visit" 55 "timeout-visit" 11}
                "02:00" {"visit" 88 "timeout-visit" 44}
                "03:00" {"visit" 70 "timeout-visit" 55}
                "04:00" {"visit" 65 "timeout-visit" 33}})
(defn -main []
  (let [chart-in (:out (sh/proc "phantomjs-2.1.1-macosx/bin/phantomjs" "snapshot.js"
                                (format "file:///Users/larluo/work/git/clojure-example/email-chart-sender/public/index.html#_b64=%s" 
                                  (->> chart-edn pr-str .getBytes base64-encode form-encode))))
        chart-html (html [:div [:h1 "hello world"] 
                         [:img {:src (format "data:image/png;base64, %s" (->> chart-in slurp-bytes base64-encode))}]])]
    #_(io/copy chart-in (io/file "/Users/larluo/larluo/test/aaa.png"))
    (send-message {:host "smtp.qq.com"
                   :user "369743584@qq.com"
                   :pass "agtqtdaihwdabiha"
                   :ssl :yes}
                  {:from "369743584@qq.com"
                   :to ["larluo@clojurians.org"]
                   :subject "email chart sender"
                   :body [{:type "text/html" :content chart-html}]})) )


(comment
  (next (clojure.string/split ""))
  (count (-> chart-edn pr-str .getBytes base64-encode form-encode))
  (-main)
  )


