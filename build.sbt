lazy val app = (project in file(".")).settings(
  name := "bully-node",
    version := "0.1",
  scalaVersion := "2.13.6",
  libraryDependencies ++= Dependencies(),
  assemblyJarName := "bullynode.jar",
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)
