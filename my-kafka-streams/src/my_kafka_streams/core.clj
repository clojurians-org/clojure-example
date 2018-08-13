(ns my-kafka-streams.core)

(do
  (require '[cheshire.core :refer [parse-string generate-string]])
  (import '[org.apache.kafka.common.serialization Serdes])
  (import '[org.apache.kafka.streams StreamsConfig StreamsBuilder KafkaStreams Topology$AutoOffsetReset])
  (import '[org.apache.kafka.streams.kstream KStream Consumed TransformerSupplier Transformer])
  (import '[org.apache.kafka.streams.processor ProcessorContext])
  )

(deftype TransducerTransformer [step-fn ^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  (transform [_ k v]
    (try
      (step-fn context [k v])
      (catch Exception e
        (.printStackTrace e)))
    nil)
  (close [_]))

(defn- kafka-streams-step
  ([context] context)
  ([^ProcessorContext context [k v]]
   (.forward context k v)
   (.commit context)
   context))

(defn transduce-kstream
  ^KStream [^KStream kstream xform]
  (.transform kstream 
    (reify
      TransformerSupplier
      (get [_] (TransducerTransformer. (xform kafka-streams-step) nil)))
    (into-array String []))
  )

(defn mk-topology [from-topic to-topic xform]
    (-> (doto (StreamsBuilder.)
          (-> (.stream from-topic (Consumed/with Topology$AutoOffsetReset/EARLIEST))
              (transduce-kstream xform)
              (.to to-topic)))
        .build)
    )

(defn mk-streams [host id from-topic to-topic xform]
    (KafkaStreams. (mk-topology from-topic to-topic xform)
                   (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG id
                                    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "10.132.37.33:9092"
                                    StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (class (Serdes/String)) 
                                    StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (class (Serdes/String)) }))
    )

(defn update-multi-in [st kf vf ms] (reduce #(update-in %1 (kf %2) (fn [x] (vf x %2))) st ms))
(defn update-st [st kdt v]
    (let [basicInfo (:basicInfo v)
          interfaceInfos (:interfaceInfos v)
          kappID  (-> basicInfo :appID keyword)]
      (-> @st
          (update-in [kdt :DATA] 
                     #(as-> % $ (merge $ {:totalTs 400}) 
                                (merge-with + $ {:allAppNum 1 :allInterfaceNum (-> v :interfaceInfos count)}) ))
          (update-in [kdt :BRANCH :app kappID :DATA] 
                     #(as-> % $ (merge-with + $ {:totalNum 1 :successNum 1}) ))
          (update-multi-in #(do [kdt :BRANCH :interface (-> % :interfaceID keyword) :DATA]) 
                     #(as-> %1 $ (merge $ {:name (:name %2)} ) 
                                 (merge-with + $ {:totalNum 1 :successNum 1} ) )
                     interfaceInfos)
          (->> (reset! st)) )
      (let [dt-data (get-in @st [kdt :DATA])
            app-data (get-in @st [kdt :BRANCH :app kappID :DATA])
            read-interface-data (fn [interface-id] (get-in @st [kdt :BRANCH :interface interface-id :DATA])) ]
        ((comp vec concat)
           [{:dt (name kdt) :type :app :id kappID 
               :allKindNum (:allAppNum dt-data) :totalTs (:totalTs dt-data) 
               :totalNum (:totalNum app-data) :successNum (:successNum app-data)}] 
           (mapv
             #(let [interfaceID (-> % :interfaceID keyword)
                    interface-data (read-interface-data interfaceID)] 
                 {:dt (name kdt) :type :interface :id interfaceID
                  :allKindNum (:allInterfaceNum dt-data) :totalTs (:totalTs dt-data)
                  :totalNum (:totalNum interface-data) :successNum (:successNum interface-data)} )
             interfaceInfos))
        ))
    )

(defn -main []
  (println "beginning...")
  (let [st (atom {})
        xform (comp 
                (map (comp #(parse-string % true) :message #(parse-string % true) second) )
                (filter #(= (:logger_name %) "APMInfoDev" ))
                (map (comp #(parse-string % true) :message))
                (mapcat #(update-st st :2018-08-12 %))
                (map #(vector nil (generate-string %))))
        kafka-streams (mk-streams "10.132.37.33:9092" "larluo-test" "logi_hop_sdk_apm"  "larluo" xform)]
      (.start kafka-streams))
  )
(comment
{:2018-01-01
   {:DATA {:totalNum 0 :totalTs 0} 
    :BRANCH {:app {:58465912-7f0c-43a6-ba4e-4ac5f60eccf8 
                     {:DATA {:name "app-58465912-7f0c-43a6-ba4e-4ac5f60eccf8" :totalNum 0 :successNum 0 :maxTps 0 :curTps 0}}}
            :interface {:1111
                         {:DATA {:name "interface-1111" :totalNum 0 :successNum 0 :maxTps 0 :curTps 0}}}}}}
  ((comp vec transduce) 
    (comp 
          (mapcat #(update-st st :2018-08-01 %)))
    conj
  [{:eventInfo {:data {}, :name "", :type ""}, :basicInfo {:appVersion "SDK_2.0.2", :appID "58465912-7f0c-43a6-ba4e-4ac5f60eccf8", :model "m2", :mobileOSVersion "5.1", :mobileOS "Android"}, :interfaceInfos [{:interfaceID "1111", :state "1111", :timeDuration 0.0}]}
   {:eventInfo {:data {}, :name "", :type ""}, :basicInfo {:appVersion "SDK_2.0.2", :appID "58465912-7f0c-43a6-ba4e-4ac5f60eccf8", :model "m2", :mobileOSVersion "5.1", :mobileOS "Android"}, :interfaceInfos [{:interfaceID "2222", :state "1111", :timeDuration 0.0}]}
   {:eventInfo {:data {}, :name "", :type ""}, :basicInfo {:appVersion "SDK_2.0.2", :appID "58465912-7f0c-43a6-ba4e-4ac5f60eccf8", :model "m2", :mobileOSVersion "5.1", :mobileOS "Android"}, :interfaceInfos [{:interfaceID "1111", :state "1111", :timeDuration 0.0} {:interfaceID "1111", :state "1111", :timeDuration 0.0}]}]
    )
  (update-multi-in {} #(do [(->  % :id str keyword)]) #(merge-with + %1 {:a %2}) [{:id 3 :b 4 :c 3} {:id 4 :b 35}] )
  (update-st st :2018-01-01
      {:eventInfo {:data {}, :name "", :type ""}, :basicInfo {:appVersion "SDK_2.0.2", :appID "58465912-7f0c-43a6-ba4e-4ac5f60eccf8", :model "m2", :mobileOSVersion "5.1", :mobileOS "Android"}, :interfaceInfos [{:interfaceID "1111", :state "1111", :timeDuration 0.0}{:interfaceID "1111", :state "1111", :timeDuration 0.0}]})
    @st
  )


