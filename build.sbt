import com.typesafe.sbt.packager.docker.Cmd
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

val scala212 = "2.12.15"
val scala213 = "2.13.8"
val supportedScalaVersions = List(scala212, scala213)
val sparkVersion = "3.3.3"
val circeVersion = "0.14.6"
val sttpVersion = "3.5.2"
val natchezVersion = "0.3.1"
val Specs2Version = "4.20.3"
val cogniteSdkVersion = "2.17.789"

val prometheusVersion = "0.16.0"
val log4sVersion = "1.10.0"

sonatypeProfileName := "com.cognite" // default is same as organization and leads to 404 on sonatypeReleaseAll

lazy val gpgPass = Option(System.getenv("GPG_KEY_PASSWORD"))

ThisBuild / scalafixDependencies += "org.typelevel" %% "typelevel-scalafix" % "0.1.4"

lazy val patchVersion = scala.io.Source.fromFile("patch_version.txt").mkString.trim

lazy val commonSettings = Seq(
  organization := "com.cognite.spark.datasource",
  organizationName := "Cognite",
  organizationHomepage := Some(url("https://cognite.com")),
  version := "3.7." + patchVersion,
  isSnapshot := patchVersion.endsWith("-SNAPSHOT"),
  crossScalaVersions := supportedScalaVersions,
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  scalaVersion := scala212, // default to Scala 2.12
  description := "Spark data source for the Cognite Data Platform.",
  licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/cognitedata/cdp-spark-datasource")),
  scalacOptions ++= Seq("-Xlint:unused", "-language:higherKinds", "-deprecation", "-feature"),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases")
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
    if (isSnapshot.value) { Some("snapshots" at nexus + "content/repositories/snapshots") }
    else { Some("releases" at nexus + "service/local/staging/deploy/maven2") }
  },
  publishMavenStyle := true,
  pgpPassphrase := {
    if (gpgPass.isDefined) { gpgPass.map(_.toCharArray) }
    else { None }
  },
  Test / fork := true,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
  // Yell at tests that take longer than 120 seconds to finish.
  // Yell at them once every 60 seconds.
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "60")
)

lazy val structType = (project in file("struct_type"))
  .settings(
    commonSettings,
    name := "cdf-spark-datasource-struct-type",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "io.scalaland" %% "chimney" % "0.6.1",
      "org.typelevel" %% "cats-core" % "2.9.0",
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    ),
  )

// Based on https://www.scala-sbt.org/1.0/docs/Macro-Projects.html#Defining+the+Project+Relationships
lazy val macroSub = (project in file("macro"))
  .dependsOn(structType)
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
  .dependsOn(structType, macroSub % Provided)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoUsePackageAsPath := true,
    commonSettings,
    name := "cdf-spark-datasource",
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
      "org.slf4j" % "slf4j-api" % "2.0.9" % Provided,
      "io.circe" %% "circe-generic" % circeVersion
        exclude("org.typelevel", "cats-core_2.12")
        exclude("org.typelevel", "cats-core_2.13"),
      "io.circe" %% "circe-generic-extras" % "0.14.3"
        exclude("org.typelevel", "cats-core_2.12")
        exclude("org.typelevel", "cats-core_2.13"),
      "org.scalatest" %% "scalatest" % "3.0.8" % Test,
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.44.v20210927" % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.log4s" %% "log4s" % log4sVersion,
      "org.tpolecat" %% "natchez-core" % natchezVersion,
      "org.tpolecat" %% "natchez-noop" % natchezVersion,
    ),
    coverageExcludedPackages := "com.cognite.data.*",
    buildInfoKeys := Seq[BuildInfoKey](organization, version, organizationName),
    buildInfoPackage := "cognite.spark.cdf_spark_datasource"
  )

lazy val fatJarShaded = project
  .enablePlugins(AssemblyPlugin)
  .dependsOn(library)
  .settings(
    commonSettings,
    name := "cdf-spark-datasource-fatjar",
    Compile / packageBin := assembly.value,
    assembly / assemblyJarName := s"${normalizedName.value}-${version.value}-jar-with-dependencies.jar",
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
    pomPostProcess := { (node: XmlNode) =>
      new RuleTransformer(new RewriteRule {
        override def transform(node: XmlNode): XmlNodeSeq = node match {
          case e: Elem if e.label == "dependency"
                          && e.child.filter(_.label == "groupId").flatMap(_.text).mkString == "com.cognite.spark.datasource"
                          && e.child.filter(_.label == "artifactId").flatMap(_.text).mkString.startsWith("cdf-spark-datasource") =>
            // Omit library artifact from pom's dependencies.
            // All sbt-assembly settings are kept here and we can't depend on
            //   Compile / packageBin := (library / assembly).value
            // as it would try to run sbt-assembly on library too and it doesn't have
            // all the configuration.
            // Otoh library itself should remain pure non-fatjar library and know nothing about sbt-assembly.
            // There could be other ways to achieve the same effect, so far this one is found and it is simple enough.
            Seq()
          case _ => node
        }
      }).transform(node).head
    }
  )

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M")
