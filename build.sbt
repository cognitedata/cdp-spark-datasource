import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

val scala212 = "2.12.12"
val scala211 = "2.11.12"
val supportedScalaVersions = List(scala212, scala211)
val sparkVersion: Option[(Long, Long)] => String = {
  case Some((2, 11)) => "2.4.7"
  case _ => "3.0.1"
}
val circeVersion: Option[(Long, Long)] => String = {
  case Some((2, 11)) => "0.12.0-M3"
  case _ => "0.13.0"
}
val sttpVersion = "1.7.2"
val Specs2Version = "4.2.0"
val artifactory = "https://cognite.jfrog.io/cognite/"
val cogniteSdkVersion = "1.4.3"
val prometheusVersion = "0.8.1"
val log4sVersion = "1.8.2"

resolvers += "libs-release" at artifactory + "libs-release/"

lazy val gpgPass = Option(System.getenv("GPG_KEY_PASSWORD"))

lazy val commonSettings = Seq(
  organization := "com.cognite.spark.datasource",
  organizationName := "Cognite",
  organizationHomepage := Some(url("https://cognite.com")),
  version := "1.4.15",
  crossScalaVersions := supportedScalaVersions,
  description := "Spark data source for the Cognite Data Platform.",
  licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/cognitedata/cdp-spark-datasource")),
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
  fork in Test := true
)

// Based on https://www.scala-sbt.org/1.0/docs/Macro-Projects.html#Defining+the+Project+Relationships
lazy val macroSub = (project in file("macro"))
  .settings(
    commonSettings,
    scalaVersion := scala212,
    publish := {},
    publishLocal := {},
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value)) % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value)) % Provided,
      "com.cognite" %% "cognite-sdk-scala" % cogniteSdkVersion
    )
  )

lazy val library = (project in file("."))
  .dependsOn(macroSub % "compile-internal, test-internal")
  .settings(
    commonSettings,
    name := "cdf-spark-datasource",
    scalastyleFailOnWarning := true,
    scalastyleFailOnError := true,
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "com.cognite" %% "cognite-sdk-scala" % cogniteSdkVersion
        // scala-collection-compat is used in TransformerF, but we don't use that,
        // and this dependency causes issues with Livy.
        exclude("org.scala-lang.modules", "scala-collection-compat_2.11")
        exclude("org.scala-lang.modules", "scala-collection-compat_2.12"),
      "org.specs2" %% "specs2-core" % Specs2Version % Test,
      "com.softwaremill.sttp" %% "async-http-client-backend-cats" % sttpVersion
        // Netty is included in Spark as jars/netty-all-4.<minor>.<patch>.Final.jar
        exclude("io.netty", "netty-buffer")
        exclude("io.netty", "netty-codec-http")
        exclude("io.netty", "netty-codec-http")
        exclude("io.netty", "netty-codec-socks")
        exclude("io.netty", "netty-handler")
        exclude("io.netty", "netty-handler-proxy")
        exclude("io.netty", "netty-resolver-dns")
        exclude("io.netty", "netty-transport-native-epoll")
        exclude("com.softwaremill.sttp", "circe_2.11")
        exclude("com.softwaremill.sttp", "circe_2.12")
        exclude("org.typelevel", "cats-effect_2.11")
        exclude("org.typelevel", "cats-effect_2.12")
        exclude("org.typelevel", "cats-core_2.11")
        exclude("org.typelevel", "cats-core_2.12"),
      "org.slf4j" % "slf4j-api" % "1.7.16" % Provided,
      "io.circe" %% "circe-generic" % circeVersion(CrossVersion.partialVersion(scalaVersion.value))
        exclude("org.typelevel", "cats-core_2.11")
        exclude("org.typelevel", "cats-core_2.12"),
      "io.circe" %% "circe-generic-extras" % circeVersion(CrossVersion.partialVersion(scalaVersion.value))
        exclude("org.typelevel", "cats-core_2.11")
        exclude("org.typelevel", "cats-core_2.12"),
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605" % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value)) % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value)) % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.log4s" %% "log4s" % log4sVersion
    ),
    mappings in (Compile, packageBin) ++= mappings.in(macroSub, Compile, packageBin).value,
    mappings in (Compile, packageSrc) ++= mappings.in(macroSub, Compile, packageSrc).value,
    coverageExcludedPackages := "com.cognite.data.*"
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, version, organizationName),
    buildInfoPackage := "BuildInfo"
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
      "org.apache.spark" %% "spark-core" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value))
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion(CrossVersion.partialVersion(scalaVersion.value))
        exclude("org.glassfish.hk2.external", "javax.inject")
    ),
    dockerBaseImage := "eu.gcr.io/cognitedata/cognite-jre:8-slim",
    dockerCommands ++= Seq(
      Cmd("ENV", s"JAVA_MAIN_CLASS=${mainClass.in(Compile).value.get}"),
      Cmd("ENV", "JAVA_APP_DIR=/opt/docker/lib")
    ),
  )

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
