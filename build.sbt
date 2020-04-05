import sbt.Keys.libraryDependencies
// give the user a nice default project!

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.github.OrangeFlag.spark",
      scalaVersion := "2.11.12",
      sparkVersion := "2.4.4"
    )),
    name := "scala-spark-dataframe",
    version := "0.0.1",

    sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,


    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.4.4",
      "org.apache.spark" %% "spark-streaming" % "2.4.4",
      "org.xerial" % "sqlite-jdbc" % "3.30.1"
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

    resolvers ++= Seq(
      "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { x => false }
  )
