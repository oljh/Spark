name := "Spark"

version := "0.1"

scalaVersion := "2.11.12"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.5.1"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch
libraryDependencies += "org.elasticsearch" % "elasticsearch" % "7.5.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" % "spark-mllib" % "2.4.5"




