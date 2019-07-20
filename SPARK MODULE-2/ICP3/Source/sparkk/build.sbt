name := "sparkk"

version := "0.1"

scalaVersion := "2.11.8"
organization := "com.databricks.blog"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1" ,
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided",
  "com.databricks" % "spark-csv_2.11" % "1.5.0"
)