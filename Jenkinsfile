@Library('jenkins-helpers@v0.1.14') _

def label = "cdp-spark-${UUID.randomUUID().toString().substring(0, 5)}"

podTemplate(label: label,
            containers: [containerTemplate(name: 'sbt',
                                           image: 'eu.gcr.io/cognitedata/openjdk8-sbt:2018-09-18-d077396',
                                           resourceRequestCpu: '1000m',
                                           resourceLimitCpu: '3800m',
                                           resourceLimitMemory: '3500Mi',
                                           envVars: [secretEnvVar(key: 'TEST_API_KEY', secretName: 'jetfire-test-api-key', secretKey: 'jetfireTestApiKey.txt'),
                                                     secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-token-cdp-spark-connector', secretKey: 'token.txt'),
                                                     // /codecov-script/upload-report.sh relies on the following
                                                     // Jenkins and GitHub environment variables.
                                                     envVar(key: 'JENKINS_URL', value: env.JENKINS_URL),
                                                     envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
                                                     envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
                                                     envVar(key: 'BUILD_URL', value: env.BUILD_URL),
                                                     envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
                                                     envVar(key: 'SBT_OPTS', value: "-Xms512M -Xmx1024M -Xss100M -XX:MaxMetaspaceSize=1024M")],
                                           ttyEnabled: true,
                                           command: '/bin/cat -')],
            envVars: [envVar(key: 'MAVEN_OPTS', value: '-Dmaven.artifact.threads=30')],
            nodeSelector: 'cloud.google.com/gke-local-ssd=true',
            volumes: [secretVolume(secretName: 'sbt-credentials', mountPath: '/sbt-credentials'),
                      configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
                      hostPathVolume(hostPath: '/mnt/disks/ssd0/ivy2', mountPath: '/root/.ivy2'),
                      hostPathVolume(hostPath: '/mnt/disks/ssd0/sbt', mountPath: '/root/.sbt'),]) {
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
        container('sbt') {
            timeout(time: 20, unit: 'MINUTES') {
                stage('Install SBT config and credentials') {
                    sh('mkdir -p /root/.sbt/1.0 && cp /sbt-credentials/credentials.sbt /root/.sbt/1.0/credentials.sbt')
                    sh('cp /sbt-credentials/repositories /root/.sbt/')
                }
                stage('Run tests') {
                    sh('sbt -Dsbt.log.noformat=true scalastyle coverage test coverageReport')
                }
                stage("Upload report to codecov.io") {
                    sh('bash </codecov-script/upload-report.sh')
                }
                stage('Build JAR file') {
                    sh('sbt -Dsbt.log.noformat=true "set test in assembly := {}" assembly')
                }
                if (env.BRANCH_NAME == 'master') {
                    stage('Deploy') {
                        sh('sbt -Dsbt.log.noformat=true publish')
                    }
                }
            }
        }
    }
}
