pipeline {
    agent {
        docker {
            image 'maven:3.6.1-jdk-8-slim'
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