import sbt._

object Dependencies {

  object Version {
    val spark       = "3.3.1"
    val deequ       = "2.0.4-spark-3.3"
    val pureconfig  = "0.17.2"
    val doobie      = "1.0.0-RC1"
    val scalatest   = "3.2.16"
    val wartremover = "3.1.3"
    val aws         = "1.12.632"
    val hadoop      = "3.3.1"
  }

  private val spark = Seq(
    "org.apache.spark" %% "spark-core"                 % Version.spark,
    "org.apache.spark" %% "spark-sql"                  % Version.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Version.spark,
    "org.apache.spark" %% "spark-sql-kafka-0-10"       % Version.spark
  )

  private val scalatest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalatest,
    "org.scalatest" %% "scalatest" % Version.scalatest % "test"
  )

  private val deequ = ("com.amazon.deequ" % "deequ" % Version.deequ)
    .exclude("org.apache.spark", "*")
    .exclude("com.chuusai", "*")
    .exclude("org.scalanlp", "breeze_")
    .exclude("junit", "junit")

  private val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.17.2"
  private val doobie        = "org.tpolecat"   %% "doobie-core" % Version.doobie
  private val databricksXml = "com.databricks" %% "spark-xml"   % "0.16.0"
  private val delta         = "io.delta"       %% "delta-core"  % "1.0.0"
  private val hadoop = "org.apache.hadoop" % "hadoop-aws"   % Version.hadoop
  private val kafka  = "org.apache.kafka" %% "kafka"        % "3.1.1"
  private val aws    = "com.amazonaws"     % "aws-java-sdk" % Version.aws

  private val wartremover =
    "org.wartremover" %% "wartremover" % Version.wartremover

  val mainDeps: Seq[ModuleID] =
    spark ++ scalatest ++ Seq(
      deequ,
      pureconfig,
      doobie,
      databricksXml,
      delta,
      kafka,
      aws,
      hadoop
    )
}
