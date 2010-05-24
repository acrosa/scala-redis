import sbt._

class RedisClientProject(info: ProjectInfo) extends DefaultProject(info) with AutoCompilerPlugins
{
  override def useDefaultConfigurations = true

  val specs = "org.scala-tools.testing" % "specs" % "1.5.0"
  val mockito = "org.mockito" % "mockito-all" % "1.7"
  val junit = "junit" % "junit" % "4.5"
}
