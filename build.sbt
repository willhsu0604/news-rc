name := "news-rc"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "com.google.guava" % "guava" % "12.0.1" force()
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.3" force()
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.3" force()
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.3" force()
libraryDependencies +=  "org.slf4j" % "slf4j-simple" % "1.7.21"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.6.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.6.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.4.7"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.4.7"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.0.0-cdh5.4.7"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.0.0-cdh5.4.7" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-testing-util" % "1.0.0-cdh5.4.7" % "provided"
libraryDependencies += "org.ansj" % "ansj_seg" % "5.1.1"
libraryDependencies += "org.nlpcn" % "nlp-lang" % "1.7.2"
libraryDependencies += "org.jsoup" % "jsoup" % "1.9.2"
libraryDependencies += "com.101tec" % "zkclient" % "0.4"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"
libraryDependencies += "org.elasticsearch.client" % "transport" % "5.2.2"

    