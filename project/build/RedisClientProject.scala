import sbt._

class RedisClientProject(info: ProjectInfo) extends DefaultProject(info) with AutoCompilerPlugins
{
  override def useDefaultConfigurations = true

  val specs = "org.scala-tools.testing" % "specs_2.8.0.Beta1" % "1.6.2"
  val mockito = "org.mockito" % "mockito-all" % "1.8.1"
  val junit = "junit" % "junit" % "4.5"
}
