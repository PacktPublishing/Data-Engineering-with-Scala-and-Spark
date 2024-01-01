addSbtPlugin("ch.epfl.scala" % "sbt-bloop"    % "1.4.6")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.25")
addSbtPlugin("com.dwijnand"  % "sbt-dynver"   % "4.1.1")
addSbtPlugin(
  ("com.scalapenos" % "sbt-prompt" % "1.0.2")
    .exclude("com.typesafe.sbt", "sbt-git")
)
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"       % "0.10.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.0")
addSbtPlugin(
  ("com.typesafe.sbt" % "sbt-git" % "1.0.0")
    .exclude("org.eclipse.jgit", "org.eclipse.jgit")
)
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.1.3")
addSbtPlugin("ch.epfl.scala"   % "sbt-bloop"       % "1.4.6")
addSbtPlugin("ch.epfl.scala"   % "sbt-scalafix"    % "0.9.25")
addSbtPlugin("com.dwijnand"    % "sbt-dynver"      % "4.1.1")
addSbtPlugin(
  ("com.scalapenos" % "sbt-prompt" % "1.0.2")
    .exclude("com.typesafe.sbt", "sbt-git")
)
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"       % "0.10.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.0")
addSbtPlugin(
  ("com.typesafe.sbt" % "sbt-git" % "1.0.0")
    .exclude("org.eclipse.jgit", "org.eclipse.jgit")
)
addSbtPlugin("org.scoverage"                    % "sbt-scoverage"  % "1.9.1")
dependencyOverrides += "org.scala-lang.modules" % "scala-xml_2.12" % "1.2.0"
addDependencyTreePlugin
