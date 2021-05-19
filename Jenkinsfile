pipeline {
  agent any
  stages {

    // STARTING ////////////////////////////////////////////////////////////////////////////////////////////

    /* Print something to the console */
    stage('Say Hello') {
      steps {
        echo 'Hello world!'
      }
    }

    // BUILDING AND PUSHING /////////////////////////////////////////////////////////////////////////

    /* Push the image to Docker Hub */
    stage('Build the JAR file') {
      steps {
        script {
          sh 'sbt clean assembly'
        }
      }
    }

    // BUILDING AND PUSHING /////////////////////////////////////////////////////////////////////////

    /* Push the image to Docker Hub */
    stage('Push to Docker Hub') {
      steps {
        script {
          docker.withRegistry('', 'Docker_Hub') {
            def customImage = docker.build("mfarmer5102/appsbymatthew-spark:latest")
            customImage.push()
          }
        }
      }
    }

  }
}
