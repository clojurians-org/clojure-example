(ns generate-java-class.core)

(do
  (require '[cheshire.core :refer [generate-string parse-string]])
  )

(gen-class :name dataserver.scheduler.model.Job :init "init" :constructors {[java.util.Map] []})
(gen-class :name dataserver.scheduler.model.Job :prefix "Job-"
           :state "state" :init "init"
           :constructors {[java.util.Map] []}
           :methods [^:static [parse [#= (java.lang.Class/forName "[B")] dataserver.scheduler.model.Job]
                      [getSchedule [] String]])

(defn Job-parse [bytes] (-> bytes String. (parse-string true) dataserver.scheduler.model.Job.))
(defn Job-init [state] [[] (atom state)])
(defn Job-getSchedule [this] (-> this.state deref :schedule))

(comment
  (def edn {"schedule "R1/2017-09-01T10:00:00+0800/PT2H ""})
  (.getSchedule (dataserver.scheduler.model.Job/parse (-> edn generate-string .getBytes)))
  )

