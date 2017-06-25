pipeline {
    agent any
    tools {
        maven 'maven'
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn -DskipTests clean org.jacoco:jacoco-maven-plugin:prepare-agent test install'
            }
        }
    }
}
