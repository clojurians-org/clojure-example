(ns xml-parser.core
  (:require [clojure.java.io :as io]
            [clojure.xml :as xml]
            [clojure.zip :as zip]
            [clj-xpath.core :refer [$x $x:tag $x:text $x:attrs $x:attrs* $x:node]]) )

(def root (slurp "/home/spiderdt/work/jing/workspace/jupiter/data/testing_fulltest.schema.larluo"))
(map #(-> % :attrs (map [:flow :type])) ($x "//analytic/dataset/fielda" root)) 
