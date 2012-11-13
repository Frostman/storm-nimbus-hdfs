## storm-nimbus-hdfsThis library provides implementation for Nimbus Storage API that stores data in HDFS.## InstallationCopy `storm-nimbus-hdfs.jar` to `/lib` folder on each Nimbus node.## ConfigurationYou should the following properties to `storm.yaml`:```nimbus.storage: "storm.HdfsNimbusStorage"nimbus.storage.hdfs.path: "hdfs://hdfs-01.vm.net:9000"nimbus.storage.hdfs.user: "hadoop"nimbus.storage.hdfs.dir: "/storm-ha/"```## CheckOn start, Nimbus wil write some info about used storage to logs.