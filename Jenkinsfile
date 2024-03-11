pipeline {
    agent { 
            docker { 
                image 'node:18-buster' 
            }
        }
    options {
        checkoutToSubdirectory('eosc-recommender-metrics')
        newContainerPerStage()
    }
    environment {
        PROJECT_DIR='eosc-recommender-metrics'
        GH_USER = 'newgrnetci'
        GH_EMAIL = '<argo@grnet.gr>'
   }
    stages {
         stage('Python syntax style checks') {
            agent {
                docker {
                    image 'argo.registry:5000/python3'
                    args '-u jenkins:jenkins'
                }
            }
            steps {
                echo 'Checking Python syntax style with flake8'
                sh """
                pipenv --python 3
                pipenv run pip install flake8
                pipenv run python -m flake8 ./
                pipenv --rm
                """
            }
            post {
                always {
                    cleanWs()
                }
            }
        }
        stage ('Deploy Docs') {
           
            when {
                anyOf {
                    branch 'master'
                    branch 'devel'
                }
            }
            steps {
                echo 'Publish eosc-recommender-metrics docs...'
                sh '''
                    cd $WORKSPACE/$PROJECT_DIR
                    cd website
                    npm install
                '''
                sshagent (credentials: ['jenkins-master']) {
                    sh '''
                        cd $WORKSPACE/$PROJECT_DIR/website
                        mkdir ~/.ssh && ssh-keyscan -H github.com > ~/.ssh/known_hosts
                        git config --global user.email ${GH_EMAIL}
                        git config --global user.name ${GH_USER}
                        GIT_USER=${GH_USER} USE_SSH=true npm run deploy
                    '''
                }
            }
        } 
    }
    post{
        always {
            cleanWs()
        }
        success {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rocket: New version of eosc-recommender-metrics docs deployed! Job: $JOB_NAME !\n <https://argoeu.github.io/eosc-recommender-metrics|See them here...>")
                }
            }
        }
        failure {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rain_cloud: Deployment of eosc-recommender-metrics docs failed! Job: $JOB_NAME")
                }
            }
        }
    }
}
