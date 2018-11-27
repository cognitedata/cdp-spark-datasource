val sparkVersion = "2.3.0"
val circeVersion = "0.9.3"
val Http4sVersion = "0.18.18"
val Specs2Version = "4.2.0"
val artifactory = "https://cognite.jfrog.io/cognite/"

resolvers += "libs-release" at artifactory + "libs-release/"
publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at artifactory + "libs-snapshot-local/")
  else
    Some("releases"  at artifactory + "libs-release-local/")
}

lazy val root = (project in file("."))
  .settings(
    organization := "com.cognite.spark.datasource",
    name := "cdp-spark-datasource",
    version := "0.1.6-SNAPSHOT",
    assemblyJarName in assembly := s"${normalizedName.value}-${version.value}-jar-with-dependencies.jar",
    scalaVersion := "2.11.12",
    scalastyleFailOnWarning := true,
    scalastyleFailOnError := true,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-client" % Http4sVersion
        exclude("org.slf4j", "slf4j-api"),
      "org.http4s" %% "http4s-circe" % Http4sVersion,
      "org.specs2" %% "specs2-core" % Specs2Version % Test,

      "com.squareup.okhttp3" % "okhttp" % "3.9.1",

      "org.slf4j" % "slf4j-api" % "1.7.16" % Provided,
      "io.circe" %% "circe-core" % circeVersion exclude("org.typelevel", "cats-core"),
      "io.circe" %% "circe-generic" % circeVersion exclude("org.typelevel", "cats-core"),
      "io.circe" %% "circe-parser" % circeVersion exclude("org.typelevel", "cats-core"),
      "io.circe" %% "circe-literal" % circeVersion exclude("org.typelevel", "cats-core"),
      "io.circe" %% "circe-generic-extras" % circeVersion exclude("org.typelevel", "cats-core"),

      "com.cognite.data" % "cognite-data" % "0.24",

      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.10.0" % Test,

      "com.groupon.dse" % "spark-metrics" % "2.1.0-cognite" % Provided,
      // TODO: check if we really need spark-hive
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject"),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
        exclude("org.glassfish.hk2.external", "javax.inject")
    )
  )

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "repackaged.com.google.protobuf.@1").inAll,
  ShadeRule.rename("io.circe.**" -> "repackaged.io.circe.@1").inAll,
  ShadeRule.rename("cats.**" -> "repackaged.cats.@1").inAll
)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

