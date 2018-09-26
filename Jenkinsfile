node('aws'){
  checkout scm
  withMaven(
      maven: 'M3',
      jdk: 'jdk8',
      mavenSettingsFilePath: 'settings.xml',
      mavenLocalRepo: '.repository') {
    // Run the maven build

     stage("Build"){
         sh "mvn clean install -DskipTests -DskipITs"
     }
     withCredentials([usernamePassword(credentialsId: 'jenkins-nexus', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
         stage("Deploy"){
             sh "mvn deploy -DskipTests -DskipITs -Dnexus.username=${USERNAME} -Dnexus.password=${PASSWORD}"
         }
     }
   }
}
