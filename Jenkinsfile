@Library('jenkins-helpers@v0.1.14') _

def label = "cdp-spark-${UUID.randomUUID().toString().substring(0, 5)}"

podTemplate(label: label,
            containers: [containerTemplate(name: 'maven',
                                           image: 'maven:3.5.2-jdk-8',
                                           resourceRequestCpu: '100m',
                                           resourceLimitCpu: '2000m',
                                           resourceRequestMemory: '3000Mi',
                                           resourceLimitMemory: '3000Mi',
                                           ttyEnabled: true,
                                           command: '/bin/cat -'),
                         containerTemplate(name: 'busybox',
                                           image: 'eu.gcr.io/cognitedata/library/busybox:1.27.2',
                                           resourceRequestCpu: '50m',
                                           resourceLimitCpu: '500m',
                                           resourceLimitMemory: '10Mi',
                                           command: '/bin/cat -',
                                           ttyEnabled: true)],
            envVars: [envVar(key: 'MAVEN_OPTS', value: '-Dmaven.artifact.threads=30')],
            volumes: [secretVolume(secretName: 'maven-credentials', mountPath: '/maven-credentials'),
                      hostPathVolume(hostPath: '/mnt/disks/ssd0/m2repository', mountPath: '/root/.m2/repository'),]) {
    properties([buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '20'))])
    node(label) {
        def imageTag
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                imageTag = sh(returnStdout: true,
                              script: 'echo \$(date +%Y-%m-%d)-\$(git rev-parse --short HEAD)').trim()
            }
        }
        container('busybox') {
            stage('Make .m2 and .m2/repository read-write for everyone') {
                sh('chmod 777 /root/.m2')
                sh('chmod 777 /root/.m2/repository')
            }
        }
        container('maven') {
            stage('Install Maven credentials') {
                sh('cp /maven-credentials/settings.xml /root/.m2')
            }
            stage('Test') {
                // TODO: mock out server responses so we don't need a real API key to run tests
                //sh('mvn test || true')
                //junit(allowEmptyResults: false, testResults: '**/target/surefire-reports/*.xml')
                //summarizeTestResults()
                sh('mvn -B verify -DskipTests')
            }
            if (env.BRANCH_NAME == 'master') {
                stage('Deploy') {
                    sh('mvn -B deploy -DskipTests')
                }
            }
        }
    }
}
