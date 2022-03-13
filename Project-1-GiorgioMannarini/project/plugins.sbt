resolvers += Resolver.jcenterRepo
addSbtPlugin("net.aichler" % "sbt-jupiter-interface" % "0.8.3")
addSbtPlugin(
  "com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "latest.release"
)
addSbtPlugin("org.jetbrains" % "sbt-ide-settings" % "1.1.0")
addSbtPlugin("com.gilcloud" % "sbt-gitlab" % "0.0.6")
