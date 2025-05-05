val Http4sVersion = "0.23.30"
val CirceVersion = "0.14.13"
val MunitVersion = "1.1.0"
val LogbackVersion = "1.5.18"
val MunitCatsEffectVersion = "2.1.0"
val SparkVersion = "3.5.1"
val PureConfig = "0.17.1"

lazy val root = (project in file("."))
  .settings(
    organization := "com.example",
    name := "data-mart",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.13.16",
    useCoursier := false,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % Http4sVersion,
      "org.http4s" %% "http4s-ember-client" % Http4sVersion,
      "org.http4s" %% "http4s-circe" % Http4sVersion,
      "org.http4s" %% "http4s-dsl" % Http4sVersion,
      "io.circe" %% "circe-generic" % CirceVersion,
      "org.scalameta" %% "munit" % MunitVersion % Test,
      "org.typelevel" %% "munit-cats-effect" % MunitCatsEffectVersion % Test,
      "ch.qos.logback" % "logback-classic" % LogbackVersion % Runtime,
      "org.apache.spark" %% "spark-core" % SparkVersion,
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      "com.github.pureconfig" %% "pureconfig" % PureConfig
    ),
    scalacOptions ++= Seq(
      "-Ywarn-unused:imports"
    ),
    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    addCompilerPlugin(
      "org.typelevel" %% "kind-projector" % "0.13.3" cross CrossVersion.full
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    assembly / assemblyMergeStrategy := {
      case "module-info.class" => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value.apply(x)
    },
    mainClass in Compile := Some("com.example.datamart.Main"),
    dockerBaseImage := "sbtscala/scala-sbt:eclipse-temurin-23.0.2_7_1.10.11_3.6.4",
    dockerUpdateLatest := true,
    dockerEnvVars := Map(
      "SBT_OPTS" -> "-Xmx3G -Xms2G",
      "JAVA_OPTS" -> "-Xmx4G -Xms3G"
    )
  )

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
