@Library('jenkins-helpers@v0.1.14') _

def label = "cdp-spark-${UUID.randomUUID().toString().substring(0, 5)}"

podTemplate(label: label,
            containers: [
                containerTemplate(
                    name: 'sbt',
                    image: 'eu.gcr.io/cognitedata/openjdk-sbt:jdk8-2020-03-20-3631d83',
                    resourceRequestCpu: '3800m',
                    resourceLimitCpu: '7800m',
                    resourceLimitMemory: '7500Mi',
                    envVars: [secretEnvVar(key: 'TEST_API_KEY_WRITE', secretName: 'jetfire-test-api-key', secretKey: 'jetfireTestApiKey.txt'),
                             secretEnvVar(key: 'TEST_API_KEY_READ', secretName: 'jetfire-test-api-key', secretKey: 'publicDataApiKey'),
                             secretEnvVar(key: 'TEST_API_KEY_GREENFIELD', secretName: 'jetfire-test-api-key', secretKey: 'greenfieldApiKey'),
                             secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'cdp-spark-connector'),
                             secretEnvVar(key: 'GPG_KEY_PASSWORD', secretName: 'sbt-credentials', secretKey: 'gpg-key-password'),
                             // /codecov-script/upload-report.sh relies on the following
                             // Jenkins and GitHub environment variables.
                             envVar(key: 'JENKINS_URL', value: env.JENKINS_URL),
                             envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
                             envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
                             envVar(key: 'BUILD_URL', value: env.BUILD_URL),
                             envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
                             envVar(key: 'SBT_OPTS', value: "-Xms512M -Xmx7g -Xss100M -XX:+UseG1GC -XX:+UseStringDeduplication -XX:+CMSClassUnloadingEnabled -Xss2M -XX:MaxMetaspaceSize=1024M")],
                    ttyEnabled: true,
                    command: '/bin/cat -'
                ),
                containerTemplate(
                    name: 'docker',
                    image: 'docker:19.03',
                    command: '/bin/cat -',
                    resourceRequestCpu: '200m',
                    resourceLimitCpu: '2000m',
                    resourceLimitMemory: '300Mi',
                    ttyEnabled: true
                )
            ],
            nodeSelector: 'cloud.google.com/gke-local-ssd=true',
            volumes: [secretVolume(secretName: 'sbt-credentials', mountPath: '/sbt-credentials'),
                      configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
                      hostPathVolume(hostPath: '/mnt/disks/ssd0/ivy2', mountPath: '/root/.ivy2'),
                      hostPathVolume(hostPath: '/mnt/disks/ssd0/sbt', mountPath: '/root/.sbt'),
                      hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
                      secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder'),
                     ]) {
    properties([buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '20'))])
    node(label) {
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                imageRevision = sh(returnStdout: true,
                              script: 'echo \$(date +%Y-%m-%d)-\$(git rev-parse --short HEAD)').trim()
                buildDate = sh(returnStdout: true, script: 'date +%Y-%m-%dT%H%M').trim()
                dockerImageName = "eu.gcr.io/cognitedata/cdf-spark-performance-bench"
                dockerImageTag = "${buildDate}-${imageRevision}"
            }
        }
        container('sbt') {
            timeout(time: 25, unit: 'MINUTES') {
                stage('Install SBT config and credentials') {
                    sh('mkdir -p /root/.sbt/1.0 && cp /sbt-credentials/credentials.sbt /root/.sbt/1.0/credentials.sbt')
                    sh('cp /sbt-credentials/repositories /root/.sbt/')
                    sh('mkdir -p /root/.sbt/gpg && cp /sbt-credentials/pubring.asc /sbt-credentials/secring.asc /root/.sbt/gpg/')
                }
                stage('Run tests') {
                    def test = "test"
                    if (env.BRANCH_NAME == 'master') {
                        test = "+test"
                    }
                    testStatus = sh(returnStatus: true, script: "sbt -Dsbt.log.noformat=true scalastyle scalafmtCheck coverage $test coverageReport")
                    junit(allowEmptyResults: false, testResults: '**/target/test-reports/*.xml')
                    if (testStatus != 0) {
                        summarizeTestResults()
                        error("Tests failed")
                    }
                }
                stage("Upload report to codecov.io") {
                    sh('bash </codecov-script/upload-report.sh')
                }
                stage('Build JAR file') {
                    def libPackage = "library/package"
                    if (env.BRANCH_NAME == 'master') {
                        libPackage = "+library/package"
                    }
                    sh('sbt -Dsbt.log.noformat=true'
                        + ' "set test in library := {}"'
                        + " $libPackage"
                        + ' performancebench/docker:stage'
                    )
                }
                if (env.BRANCH_NAME == 'master') {
                    stage('Deploy') {
                        sh('sbt -Dsbt.log.noformat=true +library/publishSigned')
                    }
                }
            }
        }
        container('docker') {
            stage("Build Docker image") {
                sh('docker images | head')
                sh('#!/bin/sh -e\n'
                       + 'docker login -u _json_key -p "$(cat /jenkins-docker-builder/credentials.json)" https://eu.gcr.io')
                sh('#!/bin/sh -e\n'
                   + "docker build --pull -t ${dockerImageName}:${dockerImageTag} performancebench/target/docker/stage/")
            }
            if (env.BRANCH_NAME == 'master') {
                stage('Build and push Docker image') {
                    sh("docker push ${dockerImageName}:${dockerImageTag}")
                }
            }
        }
    }
}
