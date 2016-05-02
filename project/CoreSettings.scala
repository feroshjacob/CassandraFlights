import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._

object CoreSettings {

  val sparkVersion = "1.5.2"
  val currentScalaVersion ="2.10.4"
  val electricVersion = "0.0.12"

  // sbt-assembly settings for building a fat jar
  lazy val sparkAssemblySettings = Seq(

    // Slightly cleaner jar name
    assemblyJarName in assembly := {
      name.value + "-" + version.value + ".jar"
    },

    // Drop these jars
    assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "commons-beanutils-core-1.8.0.jar",
        "commons-beanutils-1.7.0.jar",
        "servlet-api-2.5-20081211.jar",
        "servlet-api-2.5.jar",
        "scala-xml-2.11.0-M4.jar",
        "jsr311-api-1.1.1.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    }

  )
  val coreSettings = Seq(
    version := "0.0.1",
    scalaVersion := currentScalaVersion,
    parallelExecution in Test := false,
    organization := "com.recipegrace.electric",
    resolvers ++= Resolvers.allResolvers,
    name := "CassandraFlights",
    test in assembly := {},
    libraryDependencies ++= Seq(
"com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0",
    "com.recipegrace.electric" %% "electric" % electricVersion,
      "org.apache.commons" % "commons-lang3" % "3.4",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided")
  )

}
