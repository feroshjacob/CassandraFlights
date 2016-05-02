import sbt._

object BuildElectric extends Build {

  import CoreSettings._

  lazy val sparkproject = (project in file(".")).
    settings(coreSettings ++ sparkAssemblySettings: _*)

}



