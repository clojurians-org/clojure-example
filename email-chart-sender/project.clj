(defproject email-chart-sender "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-cljsbuild "1.1.6"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "1.8.51"]
                 [com.draines/postal "2.0.2"]
                 [prismatic/dommy "1.1.0"]
                 [hiccup "1.0.5"]
                 [hiccups "0.3.0"]
                 [ring/ring-codec "1.0.1"]
                 [me.raynes/conch "0.8.0"]]
  :cljsbuild {
    :builds [{
        :repl-listen-port 9000
        :source-paths ["public/cljs"]
        ; The standard ClojureScript compiler options:
        ; (See the ClojureScript compiler documentation for details.)
        :compiler {
          :output-to "public/js/app.js"  ; default: target/cljsbuild-main.js
          :output-dir "public/out"
          :optimizations :whitespace
          :pretty-print true}}]})
