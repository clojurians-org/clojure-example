(ns stream-join.core)

(defn key-merge-join [kf f ksize]
  (fn [rf]
    (let [pmax (new java.util.HashMap)
          pbuffer (new java.util.HashMap)]
      (fn ([] (rf))
          ([result] (rf result))
          ([result input]
            (let [k (kf input)
                  id (f input)
                  _ (when-not (.get pbuffer k) (.put pbuffer k (new java.util.ArrayList)))
                  _ (.put pmax k id)
                  buffer (.get pbuffer k)]
              (.add buffer [id input])
              (when (= (->> pbuffer vals (remove (memfn isEmpty)) count) ksize)
                (let [align-val (-> pmax vals sort first)]
                  (->> pbuffer
                       (map (fn [[k vals]] (into (array-map)
                                (keep (fn [[id input]]
                                    (when ((complement pos?) (compare id align-val)) [id {k input}]))
                                  vals))))
                       (apply merge-with merge)
                       (map (partial rf result))
                       dorun)
                  (doseq [data-list (vals pbuffer)]
                    (let [arr (->> data-list .toArray (filter (fn [[id input]] (pos? (compare id align-val)) )))]
                      (.clear data-list) (.addAll data-list arr)))))) )))))

(defn key-partition-by [kf f]
  (fn [rf]
    (let [pa (new java.util.HashMap)
          pv (new java.util.HashMap)]
      (fn ([] (rf))
          ([result]
            (let [result (if (every? (memfn isEmpty) (vals pa))
                           (rf result)
                           (doseq [a (vals pa)]
                             (let [v (vec (.toArray a))] (.clear a) (rf (unreduced (rf result v))))))]))
          ([result input]
            (let [k (kf input)
                  _ (when-not (.get pa k) (.put pa k (new java.util.ArrayList)))
                  buffer (.get pa k)
                  pval (or (.get pv k) ::none)
                  val (f input)]
              (.put pv k val)
              (if (or (identical? pval ::none) (= val pval))
                (do (.add buffer input) result)
                (let [v (vec (.toArray buffer))]
                  (.clear buffer)
                  (let [ret (rf result v)] (when-not (reduced? ret) (.add buffer input)) ret)))))))))


(sequence (comp (key-partition-by first second)
                (map (fn [messages]
                       (let [[k prt] (first messages)]
                         [k prt (reduce + (mapv #(nth % 2) messages))]))))
  [[:a :x 22 33]
   [:a :x 55 55]
   [:b :y 33 44]
   [:a :x 33 44]
   [:b :y 44 55]
   [:a :y 66 77]
   [:b :z 99 00]
   [:a :y 77 88]
   [:b :z 55 66]
   [:a :z 11 22]])

(sequence 
  (key-merge-join first second 2)
  [[:a :x 110] [:b :y 77] [:a :y 143] [:b :z 154] [:a :z 11]] )

