import sbtassembly.AssemblyPlugin.defaultShellScript

lazy val commonSettings = Seq(
  version := "0.0.2",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "LogParser"
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultShellScript))

assemblyJarName in assembly := s"${name.value}-${version.value}"
