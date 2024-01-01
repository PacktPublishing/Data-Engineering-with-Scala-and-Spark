import Dependencies._
import com.scalapenos.sbt.prompt._
import Keys._
import scala.sys.process._
import git.gitCurrentBranch
import com.typesafe.sbt.packager.docker.Cmd

lazy val projectName = IO.readLines(new File("PROJECT_NAME")).head
lazy val dockerAuth  = taskKey[Int]("docker remote login")

dockerAuth := {
  s"""docker login -u ${sys.env
      .get("DOCKERHUB_USER")
      .getOrElse("")} -p ${sys.env.get("DOCKERHUB_TOKEN").getOrElse("")}""" !
}

addCommandAlias("dp", "dockerAuth;docker:publish")

fork := true

val dockerSettings = Seq(
  dockerExposedPorts := Seq(8080),
  dockerBaseImage    := "apache/spark:v3.3.1",
  dockerAliases := dockerAliases.value.flatMap { alias =>
    Seq(
      alias
        .withRegistryHost(Some("rupambhattacharjee"))
        .withTag(Some(gitCurrentBranch.value.replace("main", "latest")))
    )
  },
  dockerUpdateLatest := true,
  dockerCommands := {
    Seq(
      Cmd("FROM", dockerBaseImage.value),
      Cmd("USER", "root"),
      Cmd("RUN", "apt-get --allow-releaseinfo-change update"),
      Cmd("RUN", "apt-get upgrade -y && rm -rf /var/cache/apt/*"),
      Cmd("RUN", "apt-get install -y curl"),
      Cmd("WORKDIR", "/opt/spark/work-dir"),
      Cmd("RUN", "mkdir conf/"),
      Cmd("ENV", "DEBIAN_FRONTEND=noninteractive"),
      Cmd("COPY", "1/opt/docker/lib/*.jar", "/opt/spark/jars/"),
      Cmd("RUN", "rm -f /opt/spark/jars/cats-kernel_2.12-2.0.0-M4.jar"),
      Cmd("RUN", "rm -f /opt/spark/jars/shapeless_2.12-2.3.3.jar"),
      Cmd(
        "COPY",
        "2/opt/docker/lib/*.jar",
        s"/app/de-with-scala-assembly-1.0.jar"
      )
    )
  }
)

val projectSettings = Seq(
  name             := projectName,
  organizationName := "Packt",
  organization     := "com.packt.de-with-scala",
  scalaVersion     := "2.12.13",
  connectInput     := true,
  libraryDependencies ++= mainDeps,
  dependencyOverrides += "org.scala-lang.modules" % "scala-xml_2.12" % "1.2.0",
  scalacOptions ++= Seq("-encoding", "utf8", "-Xlint", "-Ypartial-unification"),
  wartremoverWarnings ++= wartremover.Warts.allBut(Wart.ScalaApp),
  makePom / publishArtifact := false,
  pomIncludeRepository      := { _ => false },
  publishTo := Some(
    "Artifactory Realm sbt" at "https://dataengineeringwithscala.jfrog.io/artifactory/de-with-scala-sbt-dev-local"
  ),
  credentials += {
    val username = sys.env.get("ARTIFACTORYUSER")
    val password = sys.env.get("ARTIFACTORYTOKEN")
    Credentials(
      "Artifactory Realm",
      "dataengineeringwithscala.jfrog.io",
      username.getOrElse("dummy-user"),
      password.getOrElse("dummy-token")
    )
  },
  resolvers += "Artifactory Realm".at(
    "https://dataengineeringwithscala.jfrog.io/artifactory/de-with-scala-sbt-dev-local/"
  ),
  coverageEnabled             := false,
  assemblyJarName in assembly := "de-with-scala-assembly-1.0.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _                        => MergeStrategy.first
  },
  promptTheme := PromptTheme(
    List(
      text("[SBT] ", fg(blue)),
      gitBranch(clean = fg(green), dirty = fg(yellow))
        .padLeft("(")
        .padRight(")"),
      text(" : ", fg(226)),
      currentSbtKey(name, fg(80)),
      text(" ⇒ ", fg(red))
    )
  )
)

lazy val root = project
  .in(file("."))
  .settings(projectSettings)
  .settings(dockerSettings)
  .enablePlugins(GitVersioning)
  .enablePlugins(DockerPlugin)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
