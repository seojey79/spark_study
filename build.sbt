name := "spark-study"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.httpcomponents" % "httpclient" % "4.0-alpha4",
  "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
  "org.scala-lang" % "scala-xml" % "2.11.0-M4"
)