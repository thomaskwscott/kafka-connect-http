pipeline {
    agent {
        docker {
            image 'registry.nutmeg.co.uk:8443/docker-jenkins-agents/jdk11:latest'
        }
    }
    stages {
        stage('Build') {
            steps {
                script {
                    sh(
                            script: """
                            set +x
                            mvn -B \
                                -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
                                -P standalone \
                                clean package
                        """,
                            label: "Build"
                    )
                }
            }
        }

        stage('Publish') {
            when {
                branch 'master'
            }
            steps {
                script {
                    sh(
                            script: """
                            set +x
                            mvn -B \
                                -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
                                -P standalone \
                                deploy
                        """,
                            label: "Publish JAR"
                    )
                }
            }
        }
    }
}