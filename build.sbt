ThisBuild / scalaVersion := "3.3.6"
ThisBuild / organization := "de.ume.volcano"

lazy val root = (project in file("."))
  .settings(
    name := "volcano-wsi-forwarder",
    version := "0.1.0",
    javacOptions ++= Seq("--release", "21"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-release", "21", "-Wunused:imports"),
    resolvers ++= Seq(
      "dcm4che" at "https://www.dcm4che.org/maven2/",
      // dcm4che-parent imports the Weasis BOM from here
      "weasis" at "https://raw.githubusercontent.com/nroduit/mvn-repo/master/"
    ),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "4.2.0",
      "org.dcm4che" % "dcm4che-core" % "5.35.2",
      "com.hierynomus" % "smbj" % "0.14.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.20.1",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    assembly / assemblyJarName := "volcano-wsi-forwarder.jar",
    assembly / mainClass := Some("de.ume.volcano.wsi.forwarder.Main"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".properties") => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )
