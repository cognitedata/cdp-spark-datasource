import com.typesafe.sbt.packager.docker.Cmd
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

val scala212 = "2.12.15"
val scala213 = "2.13.8"
val supportedScalaVersions = List(scala212, scala213)
val sparkVersion = "3.3.1"
val circeVersion = "0.14.1"
val sttpVersion = "3.5.2"
val Specs2Version = "4.6.0"
val artifactory = "https://cognite.jfrog.io/cognite/"
val cogniteSdkVersion = "2.6.0-SNAPSHOT"

val prometheusVersion = "0.15.0"
val log4sVersion = "1.8.2"

lazy val gpgPass = Option(System.getenv("GPG_KEY_PASSWORD"))

ThisBuild / scalafixDependencies += "org.typelevel" %% "typelevel-scalafix" % "0.1.4"

lazy val commonSettings = Seq(
  organization := "com.cognite.spark.datasource",
  organizationName := "Cognite",
  organizationHomepage := Some(url("https://cognite.com")),
  version := "2.5.6-SNAPSHOT",
  isSnapshot := true,
  crossScalaVersions := supportedScalaVersions,
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  scalaVersion := scala212, // default to Scala 2.12
  description := "Spark data source for the Cognite Data Platform.",
  licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/cognitedata/cdp-spark-datasource")),
  libraryDependencies ++= Seq("io.scalaland" %% "chimney" % "0.5.3"),
  scalacOptions ++= Seq("-Xlint:unused", "-language:higherKinds", "-deprecation", "-feature"),
  resolvers ++= Seq(
    "libs-release".at(artifactory + "libs-release/"),
    Resolver.sonatypeRepo("snapshots")
  ),
  developers := List(
    Developer(
      id = "wjoel",
      name = "Joel Wilsson",
      email = "joel.wilsson@cognite.com",
      url = url("https://wjoel.com")
    ),
    Developer(
      id = "hakontro",
      name = "Håkon Trømborg",
      email = "hakon.tromborg@cognite.com",
      url = url("https://github.com/hakontro")
    ),
    Developer(
      id = "tapped",
      name = "Emil Sandstø",
      email = "emil.sandsto@cognite.com",
      url = url("https://github.com/tapped")
    )
  ),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/cognitedata/cdp-spark-datasource"),
      "scm:git@github.com:cognitedata/cdp-spark-datasource.git"
    )
  ),
  // Remove all additional repository other than Maven Central from POM
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  pgpPassphrase := {
    if (gpgPass.isDefined) gpgPass.map(_.toCharArray)
    else None
  },
  Test / fork := true,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
  // Yell at tests that take longer than 120 seconds to finish.
  // Yell at them once every 60 seconds.
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "60")
)

// Based on https://www.scala-sbt.org/1.0/docs/Macro-Projects.html#Defining+the+Project+Relationships
lazy val macroSub = (project in file("macro"))
  .settings(
    commonSettings,
    crossScalaVersions := supportedScalaVersions,
    publish := {},
    publishLocal := {},
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.cognite" %% "cognite-sdk-scala" % cogniteSdkVersion changing()
    )
  )

lazy val library = (project in file("."))
  .dependsOn(macroSub)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    name := "cdf-spark-datasource",
    assembly / assemblyJarName := s"${normalizedName.value}-${version.value}-jar-with-dependencies.jar",
    scalastyleFailOnWarning := true,
    scalastyleFailOnError := true,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "com.cognite" %% "cognite-sdk-scala" % cogniteSdkVersion changing(),
      "io.scalaland" %% "chimney" % "0.6.1"
        // scala-collection-compat is used in TransformerF, but we don't use that,
        // and this dependency causes issues with Livy.
        exclude("org.scala-lang.modules", "scala-collection-compat_2.12")
        exclude("org.scala-lang.modules", "scala-collection-compat_2.13"),
      "org.specs2" %% "specs2-core" % Specs2Version % Test,
      "com.softwaremill.sttp.client3" %% "async-http-client-backend-cats" % sttpVersion
        // Netty is included in Spark as jars/netty-all-4.<minor>.<patch>.Final.jar
        exclude("io.netty", "netty-buffer")
        exclude("io.netty", "netty-handler")
        exclude("io.netty", "netty-transport-native-epoll")
        exclude("com.softwaremill.sttp", "circe_2.12")
        exclude("com.softwaremill.sttp", "circe_2.13")
        exclude("org.typelevel", "cats-effect_2.12")
        exclude("org.typelevel", "cats-effect_2.13")
        exclude("org.typelevel", "cats-core_2.12")
        exclude("org.typelevel", "cats-core_2.13"),
      "org.slf4j" % "slf4j-api" % "1.7.16" % Provided,
      "io.circe" %% "circe-generic" % circeVersion
        exclude("org.typelevel", "cats-core_2.12")
        exclude("org.typelevel", "cats-core_2.13"),
      "io.circe" %% "circe-generic-extras" % circeVersion
        exclude("org.typelevel", "cats-core_2.12")
        exclude("org.typelevel", "cats-core_2.13"),
      "org.scalatest" %% "scalatest" % "3.0.8" % Test,
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.44.v20210927" % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.log4s" %% "log4s" % log4sVersion
    ),
    assemblyMergeStrategy := {
      case PathList("META-INF", _@_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assemblyShadeRules := {
      val shadePackage = "cognite.shaded"
      Seq(
        ShadeRule.rename("cats.**" -> s"$shadePackage.cats.@1").inAll,
        ShadeRule.rename("com.cognite.sdk.scala.**" -> s"$shadePackage.sdk.scala.@1").inAll,
        ShadeRule.rename("com.google.protobuf.**" -> s"$shadePackage.com.google.protobuf.@1").inAll,
        ShadeRule.rename("fs2.**" -> s"$shadePackage.fs2.@1").inAll,
        ShadeRule.rename("io.circe.**" -> s"$shadePackage.io.circe.@1").inAll,
        ShadeRule.rename("org.typelevel.jawn.**" -> s"$shadePackage.org.typelevel.jawn.@1").inAll,
        ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll,
        ShadeRule.rename("sttp.client3.**" -> s"$shadePackage.sttp.client3.@1").inAll
      )
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    Compile / packageBin / mappings ++= (macroSub / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (macroSub / Compile / packageSrc / mappings).value,
    coverageExcludedPackages := "com.cognite.data.*",
    buildInfoKeys := Seq[BuildInfoKey](organization, version, organizationName),
    buildInfoPackage := "cognite.spark"
  )

lazy val performancebench = (project in file("performancebench"))
  .enablePlugins(JavaAppPackaging, UniversalPlugin, DockerPlugin)
  .dependsOn(library)
  .settings(
    commonSettings,
    publish / skip := true,
    name := "cdf-spark-performance-bench",
    fork := true,
    libraryDependencies ++= Seq(
      "io.prometheus" % "simpleclient" % prometheusVersion,
      "io.prometheus" % "simpleclient_httpserver" % prometheusVersion,
      "io.prometheus" % "simpleclient_hotspot" % prometheusVersion,
      "org.log4s" %% "log4s" % log4sVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion
        exclude("org.glassfish.hk2.external", "javax.inject"),
    ),
    dockerBaseImage := "eu.gcr.io/cognitedata/cognite-jre:8-slim",
    dockerCommands ++= Seq(
      Cmd("ENV", s"JAVA_MAIN_CLASS=${(Compile / mainClass).value.get}"),
      Cmd("ENV", "JAVA_APP_DIR=/opt/docker/lib")
    ),
  )

lazy val cdfdump = (project in file("cdf_dump"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, UniversalPlugin)
  .dependsOn(library)
  .settings(
    commonSettings,
    publish / skip := true,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "cognite.spark.cdfdump",
    assembly / assemblyJarName := "cdf_dump.jar",
    assembly / assemblyMergeStrategy := {
      case n if n.contains("services") || n.startsWith("reference.conf") || n.endsWith(".conf") => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    name := "cdf_dump",
    fork := true,
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "4.0.1",
      "org.log4s" %% "log4s" % log4sVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion
        exclude("org.glassfish.hk2.external", "javax.inject"),
    ),
  )

lazy val fatJar = project.settings(
  commonSettings,
  name := "cdf-spark-datasource",
  Compile / packageBin := (library / assembly).value
)

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
