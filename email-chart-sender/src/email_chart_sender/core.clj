(require '[postal.core :refer [send-message]])
(require '[me.raynes.conch.low-level :as sh])

(sh/stream-to-string (sh/proc "phantomjs-2.1.1-macosx/bin/phantomjs" "snapshot.js" "public/index.html") :out)

(send-message {:host "smtp.qq.com"
                      :user "369743584@qq.com"
                      :pass "agtqtdaihwdabiha"
                      :ssl :yes}
              {:from "369743584@qq.com"
               :to ["larluo@clojurians.org"]
               :subject "why2!"
               :body "test"})
