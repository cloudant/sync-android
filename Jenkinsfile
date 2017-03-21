#!groovy

/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

def runTests(testEnv, isAndroid) {
    node(isAndroid ? 'android' : null) {
        if (isAndroid) {
            // Android tests run on static hardware so clean the dir
            deleteDir()
            testEnv.add('GRADLE_TARGET=-b AndroidTest/build.gradle uploadFixtures connectedCheck')
        } else {
            testEnv.add('GRADLE_TARGET=integrationTest')
        }
        // Unstash the built content
        unstash name: 'built'

        //Set up the environment and run the tests
        withEnv(testEnv) {
            withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: env.CREDS_ID, usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD']]) {
                try {
                    sh './gradlew -Dtest.with.specified.couch=true -Dtest.couch.username=$DB_USER -Dtest.couch.password=$DB_PASSWORD -Dtest.couch.host=$DB_HOST -Dtest.couch.port=$DB_PORT -Dtest.couch.http=$DB_HTTP -Dtest.couch.ignore.compaction=$DB_IGNORE_COMPACTION -Dtest.couch.ignore.auth.headers=true $GRADLE_TARGET'
                } finally {
                    junit '**/build/**/*.xml'
                    if (isAndroid) {
                        // Collect the device log
                        archiveArtifacts artifacts: '**/build/**/*logcat.log'
                    }
                }
            }
        }
    }
}

stage('Build') {
    // Checkout, build and assemble the source and doc
    node {
        checkout([
                $class                           : 'GitSCM',
                branches                         : scm.branches,
                doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
                extensions                       : scm.extensions + [[$class: 'CleanBeforeCheckout']],
                userRemoteConfigs                : scm.userRemoteConfigs
        ])
        sh './gradlew clean assemble'
        stash name: 'built'
    }
}

stage('QA') {
    // Define the matrix environments
    def CLOUDANT_ENV = ['TEST_ENV_NAME=Cloudant_Test','DB_HTTP=https', 'DB_HOST=clientlibs-test.cloudant.com', 'DB_PORT=443', 'DB_IGNORE_COMPACTION=true', 'CREDS_ID=clientlibs-test']
    def COUCH1_6_ENV = ['TEST_ENV_NAME=CouchDB1_6_Test','DB_HTTP=http', 'DB_HOST=cloudantsync002.bristol.uk.ibm.com', 'DB_PORT=5984', 'DB_IGNORE_COMPACTION=false', 'CREDS_ID=couchdb']
    def COUCH2_0_ENV = ['TEST_ENV_NAME=CouchDB2_0_Test','DB_HTTP=http', 'DB_HOST=cloudantsync002.bristol.uk.ibm.com', 'DB_PORT=5985', 'DB_IGNORE_COMPACTION=true', 'CREDS_ID=couchdb']
    def CLOUDANT_LOCAL_ENV = ['TEST_ENV_NAME=CloudantLocal_Test','DB_HTTP=http', 'DB_HOST=cloudantsync002.bristol.uk.ibm.com', 'DB_PORT=8081', 'DB_IGNORE_COMPACTION=true', 'CREDS_ID=couchdb']

    // Standard builds do Findbugs and test sync-android for Android and Java against Cloudant
    def axes = [
            Findbugs        : {
                node {
                    unstash name: 'built'
                    // findBugs
                    try {
                        sh './gradlew -Dfindbugs.xml.report=true findbugsMain'
                    } finally {
                        step([$class: 'FindBugsPublisher', pattern: '**/build/reports/findbugs/*.xml'])
                    }
                }
            },
            Java_Cloudant   : {
                runTests(CLOUDANT_ENV, false)
            },
            Android_Cloudant: {
                runTests(CLOUDANT_ENV, true)
            }
    ]
    // For the master branch, add additional axes to the coverage matrix for Couch 1.6, 2.0
    // and Cloudant Local
    if (env.BRANCH_NAME == "master") {
        axes.putAll(
                Java_Couch1_6: {
                    runTests(COUCH1_6_ENV, false)
                },
                Android_Couch1_6: {
                    runTests(COUCH1_6_ENV, true)
                },
                Java_Couch2_0: {
                    runTests(COUCH2_0_ENV, false)
                },
                Android_Couch2_0: {
                    runTests(COUCH2_0_ENV, true)
                },
                Java_CloudantLocal: {
                    runTests(CLOUDANT_LOCAL_ENV, false)
                },
                Android_CloudantLocal: {
                    runTests(CLOUDANT_LOCAL_ENV, true)
                }
        )
    }

    // Run the required axes in parallel
    parallel(axes)
}

// Publish the master branch
stage('Publish') {
    if (env.BRANCH_NAME == "master") {
        node {
            checkout scm // re-checkout to be able to git tag
            unstash name: 'built'
            // read the version name and determine if it is a release build
            version = readFile('VERSION').trim()
            isReleaseVersion = !version.toUpperCase(Locale.ENGLISH).contains("SNAPSHOT")

            // Upload using the ossrh creds (upload destination logic is in build.gradle)
            withCredentials([usernamePassword(credentialsId: 'ossrh-creds', passwordVariable: 'OSSRH_PASSWORD', usernameVariable: 'OSSRH_USER'), usernamePassword(credentialsId: 'signing-creds', passwordVariable: 'KEY_PASSWORD', usernameVariable: 'KEY_ID'), file(credentialsId: 'signing-key', variable: 'SIGNING_FILE')]) {
                sh './gradlew -Dsigning.keyId=$KEY_ID -Dsigning.password=$KEY_PASSWORD -Dsigning.secretKeyRingFile=$SIGNING_FILE -DossrhUsername=$OSSRH_USER -DossrhPassword=$OSSRH_PASSWORD upload'
            }

            // if it is a release build then do the git tagging
            if (isReleaseVersion) {

                // Read the CHANGES.md to get the tag message
                changes = """"""
                changes += readFile('CHANGES.md')
                tagMessage = """"""
                for (line in changes.readLines()) {
                    if (!"".equals(line)) {
                        // append the line to the tagMessage
                        tagMessage = "${tagMessage}${line}\n"
                    } else {
                        break
                    }
                }

                // Use git to tag the release at the version
                try {
                    // Awkward workaround until resolution of https://issues.jenkins-ci.org/browse/JENKINS-28335
                    withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'github-token', usernameVariable: 'GIT_USERNAME', passwordVariable: 'GIT_PASSWORD']]) {
                        sh "git config user.email \"nomail@hursley.ibm.com\""
                        sh "git config user.name \"Jenkins CI\""
                        sh "git config credential.username ${env.GIT_USERNAME}"
                        sh "git config credential.helper '!echo password=\$GIT_PASSWORD; echo'"
                        sh "git tag -a ${version} -m '${tagMessage}'"
                        sh "git push origin ${version}"
                    }
                } finally {
                    sh "git config --unset credential.username"
                    sh "git config --unset credential.helper"
                }
            }
        }
    }
}
