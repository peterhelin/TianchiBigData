organization := "com.packt"

name := "TianchiBigData"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.databricks" %% "spark-csv" % "1.3.0",
  "org.scalanlp" %% "breeze" % "0.11.2",
  //Optional - the 'why' is explained in the How it works
  "org.scalanlp" %% "breeze-natives" % "0.11.2"
)