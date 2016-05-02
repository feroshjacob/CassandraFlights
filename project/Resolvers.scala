
import sbt._

object Resolvers {
  val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"
  val recipegrace = "Recipegrace repo" at "http://www.recipegrace.com/nexus/content/repositories/releases/"

  val allResolvers = Seq(mvnrepository,recipegrace)

}
