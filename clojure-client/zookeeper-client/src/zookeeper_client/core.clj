(ns zookeeper-client.core
  (:require [zookeeper :as zk]))

(def client (zk/connect "192.168.1.3:2181"))

(zk/delete-all client "/chronos")


