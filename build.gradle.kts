plugins { id("aiven-apache-kafka-connectors-all.java-conventions") }

tasks.register<Exec>("execVale") {
    description = "Executes the Vale text linter"
    group = "Documentation"
    executable("/usr/bin/docker")
    args("run", "--rm", "-v", "${project.rootDir}:/project:Z", "-v", "${project.rootDir}/.github/vale/styles:/styles:Z", "-v", "${project.projectDir}:/docs:Z", "-w", "/docs", "jdkato/vale", "--filter=warn.expr", "--config=/project/.vale.ini", "--glob=!**build**", ".")
}


