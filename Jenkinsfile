pipeline {
    agent any

    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhub-creds') // Jenkins credential ID
        DOCKERHUB_REPO = "tharak397"
        APP_NAME = "log-monitoring"
        TAG = "latest"   // you can also use "${env.BUILD_NUMBER}" for unique tags
    }

    stages {
        stage('Checkout') {
            steps {
                git url: 'https://github.com/venkattharakram/Logs_with_clientName.git', branch: 'master'
            }
        }

        stage('Docker Login') {
            steps {
                sh """
                echo $DOCKERHUB_CREDENTIALS_PSW | docker login -u $DOCKERHUB_CREDENTIALS_USR --password-stdin
                """
            }
        }

        stage('Build & Tag Docker Images') {
            steps {
                script {
                    // Build log-listener
                    sh """
                    docker build -t $DOCKERHUB_REPO/${APP_NAME}-listener:$TAG ./log-listener
                    """

                    // Build log-generator
                    sh """
                    docker build -t $DOCKERHUB_REPO/${APP_NAME}-generator:$TAG ./log-generator
                    """
                }
            }
        }

        stage('Push to DockerHub') {
            steps {
                script {
                    sh "docker push $DOCKERHUB_REPO/${APP_NAME}-listener:$TAG"
                    sh "docker push $DOCKERHUB_REPO/${APP_NAME}-generator:$TAG"
                }
            }
        }

        stage('Update Docker Compose') {
            steps {
                script {
                    // Replace local build with DockerHub images dynamically
                    sh """
                    sed -i 's|build: ./log-listener|image: $DOCKERHUB_REPO/${APP_NAME}-listener:$TAG|' docker-compose.local.yml
                    sed -i 's|build: ./log-generator|image: $DOCKERHUB_REPO/${APP_NAME}-generator:$TAG|' docker-compose.local.yml
                    """
                }
            }
        }

        stage('Docker Compose Down') {
            steps {
                sh 'docker compose -f docker-compose.local.yml down'
            }
        }

        stage('Docker Compose Up') {
            steps {
                sh 'docker compose -f docker-compose.local.yml up -d'
            }
        }
    }

    post {
        always {
            sh 'docker logout'
        }
    }
}
