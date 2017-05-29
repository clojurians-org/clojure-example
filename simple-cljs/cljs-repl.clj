(require 'cljs.repl)
(require 'cljs.build.api)
(require 'cljs.repl.browser)

(cljs.build.api/build "cljs"
  {:main 'app
   :output-dir "out"
   :output-to "js/app.js"
   :verbose true})

(cljs.repl/repl (cljs.repl.browser/repl-env)
  :watch "cljs"
  :output-dir "out")

