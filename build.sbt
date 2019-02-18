val sparkVersion = "2.4.0"
val circeVersion = "0.9.3"
val Specs2Version = "4.2.0"
val artifactory = "https://cognite.jfrog.io/cognite/"

resolvers += "libs-release" at artifactory + "libs-release/"
resolvers += "cognite" at "https://repository.dev.cognite.ai/repository/all-releases/"
lazy val commonSettings = Seq(
  organization := "com.cognite.spark.datasource",
  version := "0.3.7-SNAPSHOT",
  scalaVersion := "2.11.12",
  fork in Test := true,
  publishTo := {
    if (isSnapshot.value)
      Some("snapshots" at artifactory + "libs-snapshot-local/")
    else
      Some("releases"  at artifactory + "libs-release-local/")
  }
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

lazy val library = (project in file("."))
  .settings(
    commonSettings,
    name := "cdp-spark-datasource",
    assemblyJarName in assembly := s"${normalizedName.value}-${version.value}-jar-with-dependencies.jar",
    scalastyleFailOnWarning := true,
    scalastyleFailOnError := true,
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  
      "org.specs2" %% "specs2-core" % Specs2Version % Test,

      "com.softwaremill.sttp" %% "core" % "1.5.0",
      "com.softwaremill.sttp" %% "circe" % "1.5.0",
      "com.softwaremill.sttp" %% "async-http-client-backend-cats" % "1.5.0",

      "org.slf4j" % "slf4j-api" % "1.7.16" % Provided,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-literal" % circeVersion,
      "io.circe" %% "circe-generic-extras" % circeVersion,
      
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,

      "com.groupon.dse" % "spark-metrics" % "2.4.0-cognite" % Provided,
      // TODO: check if we really need spark-hive
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject")
    ),
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "repackaged.cognite_spark_datasource.com.google.protobuf.@1").inAll,
      ShadeRule.rename("io.circe.**" -> "repackaged.cognite_spark_datasource.io.circe.@1").inAll,
      ShadeRule.rename("cats.**" -> "repackaged.cognite_spark_datasource.cats.@1").inAll,
      ShadeRule.rename("shapeless.**" -> "repackaged.cognite_spark_datasource.shapeless.@1").inAll,
      ShadeRule.rename("jawn.**" -> "repackaged.cognite_spark_datasource.jawn.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case "io.netty.versions.properties" => MergeStrategy.first
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

lazy val fatJar = project.settings(
  commonSettings,
  name := "cdp-spark-datasource-fat",
  packageBin in Compile := (assembly in (library, Compile)).value
)

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
