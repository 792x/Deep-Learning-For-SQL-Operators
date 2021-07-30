name := "framework"

version := "0.1"

//scalaVersion := "2.11.12"
//
//val sparkVersion = "2.4.5"

scalaVersion := "2.12.12"

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "au.com.bytecode" % "opencsv" % "2.4"
//  "com.opencsv" % "opencsv" % "5.5.1"
)
