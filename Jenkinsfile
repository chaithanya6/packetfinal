pipeline {
    agent any
 
    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhub-creds')
        DOCKERHUB_REPO = "mudam5"
        TAG = "latest"
        GIT_REPO = "https://github.com/chaithanya6/packetfinal.git"  // <-- change this
        GIT_BRANCH = "dev"                                    // <-- change branch if needed
    }
 
    stages {
        stage('Checkout Code') {
            steps {
                git branch: "${env.GIT_BRANCH}", url: "${env.GIT_REPO}"
            }
        }
 
        stage('Docker Login') {
            steps {
                sh '''
                echo $DOCKERHUB_CREDENTIALS_PSW | docker login -u $DOCKERHUB_CREDENTIALS_USR --password-stdin
                '''
            }
        }
 
        stage('Build Docker Images') {
            steps {
                
                    script {
                        def services = [
                            'log-collector', 'persistor-auth', 'persistor-payment',
                            'persistor-system', 'persistor-application', 'log-ui'
                        ]
                        for (service in services) {
                            sh "docker build -t $DOCKERHUB_REPO/${service}:$TAG ./${service}"
                        }
                    }
                }
            
        }
 
        stage('Push Docker Images') {
            steps {
                script {
                    def services = [
                        'log-collector', 'persistor-auth', 'persistor-payment',
                        'persistor-system', 'persistor-application', 'log-ui'
                    ]
                    for (service in services) {
                        sh "docker push $DOCKERHUB_REPO/${service}:$TAG"
                    }
                }
            }
        }
 
        stage('Update Docker Compose') {
            steps {
              
                    script {
                        def services = [
                            'log-collector', 'persistor-auth', 'persistor-payment',
                            'persistor-system', 'persistor-application', 'log-ui'
                        ]
                        for (service in services) {
                            sh "sed -i 's|build: ./${service}|image: $DOCKERHUB_REPO/${service}:$TAG|' docker-compose.cloud.yml"
                        }
                    }
                
            }
        }
 
        stage('Docker Compose Down') {
            steps {
                
                    sh 'docker compose -f docker-compose.cloud.yml down'
                }
            
        }
 
        stage('Docker Compose Up') {
            steps {
               
                    sh 'docker compose -f docker-compose.cloud.yml up -d'
                }
            
        }
    }
 
    post {
        always {
            sh 'docker logout'
        }
    }
}
