package com.packt.dewithscala

import pureconfig._
import pureconfig.generic.auto._

import cats.syntax.all._
final case class Opaque(value: String) extends AnyVal {
  override def toString = "****"
}

final case class Database(
    driver: String,
    name: String,
    scheme: String,
    host: String,
    port: String,
    username: Opaque,
    password: Opaque
)

final case class ProjectConfig(db: List[Database])

object Config {

  val cfg: ProjectConfig = ConfigSource.default.loadOrThrow[ProjectConfig]

  def getDB(name: String): Option[Database] =
    cfg.db.find(_.name === name)
}
