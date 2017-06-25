pipeline {
    agent any
    tools {
        maven 'maven'
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn clean org.jacoco:jacoco-maven-plugin:prepare-agent test package'
            }
        }
    }
}
