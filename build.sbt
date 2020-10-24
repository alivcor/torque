scalaVersion := "2.11.5"

name := "torque"
organization := "com.iresium"

version := "1.0"
retrieveManaged := true
fork in (Compile, run) := true

javaOptions in run ++= Seq("")
logLevel := Level.Error

resolvers += "confluent" at "https://packages.confluent.io/maven"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.avro" %% "avro" % "1.8.1"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
