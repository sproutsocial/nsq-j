// We are using a Jenkinsfile to deploy nsqauthj to k8s instead of oak
// because Oak will be adopting restrictions that limit what namespaces
// may be mutated. nsqauthj must run in a privileged namespace.
//
// Much of this is lifted verbatim from oak's codebase.
// Future refactoring / code reuse TBD.

def nsqauthjImage = "412335208158.dkr.ecr.us-east-1.amazonaws.com/nsqauthj"
def graphiteUrl = "http://carbon-useast1.int.sproutsocial.com/events/"
def slackChannel = "#eng-dbre"

def buildUser;

@NonCPS
def getBuildUser() {
    return currentBuild.rawBuild.getCause(Cause.UserIdCause).getUserId()
}


def validateImageExists(String imageName, String imageTag) {
    def parts = imageName.split('.dkr.ecr.us-east-1.amazonaws.com/')
    if (parts.length < 2) {
        error("Invalid image_name ${imageName}. Please use the fulll image name included the registry URL")
    }
    def registryId = parts[0]
    def repositoryName = parts[1]
    if (0 != sh(script: "aws --region us-east-1 ecr describe-images --registry-id ${registryId} --repository-name ${repositoryName} --image-ids imageTag=${imageTag}", returnStatus: true)) {
        error("Image ${imageName}:${imageTag} does not exist")
    }
}

def getDeployments(String manifest) {
    yamlData = readYaml(file: manifest)
    if (!(yamlData instanceof List)) {
        yamlData = [yamlData]
    }
    filtered = yamlData.findAll {yaml-> yaml["kind"] == "Deployment"}
    return filtered
}

// Returns a version of kustomize patch data we use to inject
// datadog data to allow for unified service tagging
def getPatches(deployments, String imageTag, String buildNumber) {
    return deployments.collect{ deployment->
        def deploymentName = deployment["metadata"]["name"]

        def containers = deployment["spec"]["template"]["spec"]["containers"]

        def containerNames = containers.collect{ it["name"] }

        return [
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": [
                "name": "${deploymentName}",
                "labels": [
                    "tags.datadoghq.com/env": "production",
                    "tags.datadoghq.com/service": "${deploymentName}",
                    "tags.datadoghq.com/version": "${imageTag}",
                ]
            ],
            "spec": [
                "template": [
                    "metadata": [
                        "annotations": [
                            "buildNumber": "${buildNumber}"
                        ],
                        "labels": [
                            "tags.datadoghq.com/env": "production",
                            "tags.datadoghq.com/service": "${deploymentName}",
                            "tags.datadoghq.com/version": "${imageTag}",
                            "version": "${imageTag}",
                        ]
                    ],
                    "spec": [
                        "containers": containerNames.collect{ containerName -> return [
                            "name": "${containerName}",
                            "env": [[
                                "name": "DD_ENV",
                                "valueFrom": [
                                    "fieldRef": [
                                        "fieldPath": "metadata.labels['tags.datadoghq.com/env']"
                                    ]
                                ]
                            ], [
                                "name": "DD_SERVICE",
                                "valueFrom": [
                                    "fieldRef": [
                                        "fieldPath": "metadata.labels['tags.datadoghq.com/service']"
                                    ]
                                ]
                            ], [
                                "name": "DD_VERSION",
                                "valueFrom": [
                                    "fieldRef": [
                                        "fieldPath": "metadata.labels['tags.datadoghq.com/version']"
                                    ]
                                ]
                            ], [
                                "name": "DD_AGENT_HOST",
                                "valueFrom": [
                                    "fieldRef": [
                                        "fieldPath": "status.hostIP"
                                    ]
                                ]
                            ]]
                        ]}
                    ]
                ]
            ]
        ]
    }
}



pipeline {
    agent { label 'docker' }

    parameters {
        string(name: 'image_tag', description: 'The tag of nsqauthj to deploy')
    }

    stages {
        stage('Verify parameters and initialize state') {
            steps {
                script {
                    if (params.image_tag == null) {
                        error("image_tag is required.")
                    }
                    buildUser = getBuildUser()
                }
            }
        }

        stage('Verify Image') {
            steps {
                script {
                    sh ecrLogin()
                    validateImageExists(nsqauthjImage, params.image_tag)
                }
            }
        }

        stage('Deploy') {
            steps {
                script {
                    // announce start of deploy
                    slackSend(
                        channel: slackChannel,
                        message: "*${buildUser}* is deploying nsqauthj *${params.image_tag}* to k8s-infra-platform (<${env.BUILD_URL}|#${env.BUILD_NUMBER}>)...",
                        color: "gray"
                    )

                    // substitute in image_tag and build_number
                    def file = "kubernetes/prod.yaml"
                    echo "envsubst against ${file}"
                    sh """#!/usr/bin/env bash
                        IMAGE_TAG=${params.image_tag} BUILD_NUMBER=${env.BUILD_NUMBER} envsubst '\${IMAGE_TAG}\${BUILD_NUMBER}' < ${file} > subbed.txt
                        mv subbed.txt ${file}
                        """

                    // Filter out Deployment resources, write a matching patch
                    def deployment = getDeployment(file)
                    def patches = getPatches(deployments, params.image_tag, env.BUILD_NUMBER)
                    def patchPaths = []

                    patches.eachWithIndex { patch,idx->
                        def patchFilename = "patch_deployment_${idx}.yaml"
                        writeYaml(file: patchFilename, data: patch, overwrite: true)
                        patchPaths.push(patchFilename)
                    }

                    // Build and write kustomization.yaml
                    def kustomizeData = [:]
                    kustomizeData['apiVersion'] = "kustomize.config.k8s.io/v1beta1"
                    kustomizeData['kind'] = "Kustomization"
                    kustomizeData['resources'] = files
                    kustomizeData['patches'] = patchPaths.collect { ["path": "${it}" ] }

                    // Let's dance: Apply customizations to the infra platform K8s cluster
                    withKubeConfig([credentialsId: "k8s-infra-platform-kubeconfig-file", namespace: "admin"]) {
                        sh "kubectl config view"
                        // Display converged manifest
                        sh "kubectl kustomize ."
                        sh "kubectl apply -k ."
                    }
                }
            }
        }
        stage('Track Deploy') {
            steps {
                script {
                    def eventBody = "${buildUser} deploy ${nsqauthjImage}:${params.image_tag} image to k8s-infra-platform Kubernetes Cluster"
                    sh "echo ${eventBody}"
                    sh "curl -H \"content-type: application/json\" " +
                        "-d '{\"what\":\"${eventBody}\",\"tags\":[\"${nsqauthjImage}\",\"deploy"],\"data\":\"\"}' " +
                        "${graphiteUrl}"
                    sh "echo Getting status of deployment"

                    def status

                    withKubeConfig([credentialsId: "k8s-infra-platform-kubeconfig-file", namespace: "admin"]) {
                        DEPLOYMENT_EXISTS = sh(script: "kubectl get deployments nsqauthj", returnStatus: true)
                        if(DEPLOYMENT_EXISTS == 0) {
                            status = sh(script: "kubectl rollout status deployments/nsqauthj --timeout=1200s", returnStatus: true)
                        } else {
                            status = 0
                            sh "echo \" No deployment found matching nsqauthj - This may be because deployment name doesn't match service, or because it failed to deploy.\""
                        }
                    }

                    if (STATUS == 0) {
                        slackSend(channel: slackChannel, message: "*${buildUser}'s* deploy of nsqauthj ${nsqauthjImage}* to k8s-infra-platform was successful.", color: "good")
                    } else {
                        slackSend(channel: slackChannel, message: "*${buildUser}'s* deploy of nsqauthj ${nsqauthjImage}* to k8s-infra-platform failed.", color: "bad")
                        error("kubectl rollout status returned non-zero exit code ${status}")
                    }
                }
            }
        }
    }
}