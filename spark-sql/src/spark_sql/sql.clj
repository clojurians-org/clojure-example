(ns spark-sql.core)

(do
  (import '[org.apache.hadoop.conf Configuration])
  (import '[org.apache.hadoop.yarn.conf YarnConfiguration])
  (import '[org.apache.curator.framework CuratorFrameworkfFactory])
  (import '[org.apache.curator.retry ExponentialBackoffRetry])

  (import '[org.apache.hadoop.hdfs.servernamenode.ha.proto HAZKInfoProtos$ActiveNodeInfo])
  (import '[org.apache.hadoop.yarn.proto YarnServerResourceManagerServiceproto$ActiveRMInfoProto])
  (import '[org.apache.hadoop.yarn.client.api.impl YarnClientImpl])

  (import '[org.apache.spark.sql SparkSession])
  )

(defn read-hdfs-master [zk-url hdfs-name]
  (with-open [zk-con (doto (CuratorFrameworkFactory/newClient zk-url (new ExponentialBackoffRetry 1000 60)) .start)]
    (-> zk-con .getData (.forPath (format "/hadoop-ha/%s/ActiveStandbyElectorLock" hdfs-name)) HAZKInfoProtos$ActiveNodeInfo/parseFrom bean :hostname))
  )

(defn read-yarn-master [zk-url yarn-name rm-kv]
  (with-open [zk-con (doto (CuratorFrameworkFactory/newClient zk-url (new ExponentialBackoffRetry 1000 60)) .start)]
    (-> zk-con .getData (.forPath (format "/yarn-leader-election/%s/ActiveStandbyElectorLock" yarn-name)) YarnServerResourceManagerServiceProtos$ActiveRMInfoProto/parseFrom bean :rmId keyword rm-kv))
  )

(comment
  (def hdfs-master (read-hdfs-master "hadoop...2181" "hdp"))
  (def yarn-master (read-yarn-master "hadoop...2181" "yarn-cluster" {:rm1 "hadoop002.xxx.com" :rm2 "hadoop003.xxx.com"}))

  (def spark (-> (SparkSession/builder)
                 (.appName "spark-sql-test")
                 (.config "spark.master" "local")
                 (.config "spark.hadoop.yarn.resourcemanager.hostname" yarn-master)
                 (.config "spark.hadoop.ha.zookeeper.quorum" "hadoop....2181")
                 (.config "spark.hadoop.fs.defaultFS" "hdfs://hdp")
                 (.config "spark.hadoop.dfs.client.failover.proxy.provider.hdp" "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
                 (.config "spark.hadoop.dfs.namenode.rpc-address.hdp.nn1" "hadoop...8020")
                 (.config "spark.hadoop.dfs.namenode.rpc-address.hdp.nn2" "hadoop...8020")
                 (.config "hive.metastore.uris" "thrift://...9083")
                 (.config "spark.sql.warehouse.dir" "/apps/hive/warehouse")
                 .enableHiveSupport .getOrCreate))

  (-> spark (.sql "select count(*) from a limit 10") .show)
  (def yarn-con (doto (new YarnClientImpl)
                      (.init (new YarnConfiguration (doto (new Configuration (.set "yarn.resourcemanager.address" yarn-master)))))))
  (-> yarn-con .getApplications count)
  )
