
name := "wiki_data_fetcher"
version := "1.0"
scalaVersion := "2.10.4"

resolvers += "jitpack" at "https://jitpack.io"

val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1"
val sparkCoreDep = "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
val sparkSQLDep = "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"
val sparkDataBricksDep = "com.databricks" %% "spark-xml" % "0.3.3"
val mongoDBHadoopCore = ("org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.1")
  .exclude("commons-logging", "commons-logging")
  .exclude("commons-beanutils", "commons-beanutils-core")
  .exclude("commons-collections", "commons-collections")

val testDependencies = Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.commons" % "commons-configuration2" % "2.1" % "test",
  "commons-beanutils" % "commons-beanutils" % "1.9.2" % "test"
)

libraryDependencies ++= testDependencies
libraryDependencies ++= Seq(
  mongoDBDriverDep,
  sparkCoreDep,
  sparkSQLDep,
  sparkDataBricksDep,
  "com.github.WikiPlag" % "wikiplag_utils" % "-SNAPSHOT",
  "commons-cli" % "commons-cli" % "1.2",
  mongoDBHadoopCore
)

assemblyJarName in assembly := "wiki_data_fetcher.jar"
