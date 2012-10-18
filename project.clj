(defproject storm-nimbus-hdfs "0.1.0-wip1"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [
                 [org.apache.hadoop/hadoop-client "0.20.2-cdh3u5"
                  :exclusions [commons-codec/commons-codec
                               hsqldb/hsqldb
                               oro/oro
                               commons-net/commons-net
                               org.codehaus.jackson/jackson-mapper-asl
                               org.codehaus.jackson/jackson-core-asl
                               xmlenc/xmlenc
                               ]]
                ]
  :dev-dependencies [
                     [org.clojure/clojure "1.4.0"]
                     [org.clojars.frostman/storm "0.8.2-wip11-storage"]
                     [log4j/log4j "1.2.16"]
                    ]
  :aot :all
)
