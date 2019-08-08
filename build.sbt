val scala212 = "2.12.8"
val scala211 = "2.11.12"
val supportedScalaVersions = List(scala212, scala211)
val sparkVersion = "2.4.3"
val circeVersion = "0.11.1"
val sttpVersion = "1.6.3"
val Specs2Version = "4.2.0"
val artifactory = "https://cognite.jfrog.io/cognite/"

resolvers += "libs-release" at artifactory + "libs-release/"

lazy val gpgPass = Option(System.getenv("GPG_KEY_PASSWORD"))

lazy val commonSettings = Seq(
  organization := "com.cognite.spark.datasource",
  organizationName := "Cognite",
  organizationHomepage := Some(url("https://cognite.com")),
  version := "0.4.12",
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

// Added this to fix a race condition between ProtoBuf and BuildInfo
// https://github.com/sbt/sbt/issues/1583#issuecomment-262166588
managedSourceDirectories in Compile += target.value / "protobuf-generated"

PB.targets in Compile := Seq(
  scalapb.gen() -> (target.value / "protobuf-generated")
)

// Based on https://www.scala-sbt.org/1.0/docs/Macro-Projects.html#Defining+the+Project+Relationships
lazy val macroSub = (project in file("macro"))
  .settings(
    commonSettings,
    publish := {},
    publishLocal := {},
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided)
  )

lazy val library = (project in file("."))
  .dependsOn(macroSub % "compile-internal, test-internal")
  .settings(
    commonSettings,
    name := "cdp-spark-datasource",
    scalastyleFailOnWarning := true,
    scalastyleFailOnError := true,
    libraryDependencies ++= Seq(

      "com.cognite" %% "cognite-sdk-scala" % "0.1.1",

      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",

      "org.specs2" %% "specs2-core" % Specs2Version % Test,

      "com.softwaremill.sttp" %% "async-http-client-backend-cats" % sttpVersion
        exclude("io.netty", "netty-transport-native-epoll"),

      "org.slf4j" % "slf4j-api" % "1.7.16" % Provided,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-generic-extras" % circeVersion,

      "org.scalatest" %% "scalatest" % "3.0.5" % Test,

      "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605" % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject")
    ),
    mappings in (Compile, packageBin) ++= mappings.in(macroSub, Compile, packageBin).value,
    mappings in (Compile, packageSrc) ++= mappings.in(macroSub, Compile, packageSrc).value,
    coverageExcludedPackages := "com.cognite.data.*"
  )
  .enablePlugins(
    BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, version, organizationName),
    buildInfoPackage := "BuildInfo"
  )

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
