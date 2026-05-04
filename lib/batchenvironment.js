const BbPromise = require('bluebird');
const _ = require('lodash');

/**
 * @returns {string} "IamRoleBatchService"
 */
function getBatchServiceRoleLogicalId() {
    return "IamRoleBatchService";
}

/**
 * @returns {string} "IamRoleBatchInstanceManagement"
 */
function getBatchInstanceManagementRoleLogicalId() {
    return "IamRoleBatchInstanceManagement";
}

/**
 * @returns {string} "IamProfileBatchInstanceManagement"
 */
function getBatchInstanceManagementProfileLogicalId() {
    return "IamProfileBatchInstanceManagement";
}

/**
 * @returns {string} "IamRoleBatchSpotFleetManagement"
 */
function getBatchSpotFleetManagementRoleLogicalId() {
    return "IamRoleBatchSpotFleetManagement";
}

/**
 * @returns {string} "IamRoleLambdaScheduleExecution"
 */
function getLambdaScheduleExecutionRoleLogicalId() {
    return "IamRoleLambdaScheduleExecution";
}

/**
 * @returns {string} "BatchComputeEnvironment"
 */
function getBatchComputeEnvironmentLogicalId() {
    return "BatchComputeEnvironment";
}

/**
 * @returns {string} "BatchJobQueue"
 */
function getBatchJobQueueLogicalId() {
    return "BatchJobQueue";
}

/**
 * @returns {string} The name of the job queue to be used when submitting the job
 */
function getBatchJobQueueName() {
    return `${this.provider.serverless.service.service}-${this.provider.getStage()}-JobQueue`;
}

/**
 * Validates the "batch" object in the serverless config to ensure that we have:
 *  - subnets
 *  - securityGroups
 */
function validateAWSBatchServerlessConfig() {
    const provider = this.serverless.service.provider;
    if (!provider.hasOwnProperty("batch")) {
        throw new Error("'batch' configuration not defined on the provider");
    }

    const batch = provider.batch;
    if (!batch.hasOwnProperty("SecurityGroupIds")) {
        throw new Error("'batch' configuration does not contain property 'SecurityGroupIds' (make sure it's capitalized)");
    }
    if (!batch.hasOwnProperty("Subnets")) {
        throw new Error("'batch' configuration does not contain property 'Subnets' (make sure it's capitalized)");
    }
}

/**
 * Generates the IAM Service Role Object to be used by the Batch Compute Environment
 */
function generateBatchServiceRole() {
    const batchServiceRoleName = `BatchServiceRole-${this.provider.getRegion()}-${this.provider.getStage()}-${this.provider.serverless.service.service}`.substring(0, 64);
    const batchServiceRoleTemplate = `
    {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "${batchServiceRoleName}",
        "Path": "/",
        "AssumeRolePolicyDocument": {
          "Version": "2008-10-17",
          "Statement": [
            {
              "Sid": "",
              "Effect": "Allow",
              "Principal": {
                "Service": "batch.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        },
        "ManagedPolicyArns": [
          "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
        ]
      }
    }
    `;

    return {
        [this.provider.naming.getBatchServiceRoleLogicalId()]: JSON.parse(batchServiceRoleTemplate)
    };
}

/**
 * Generates the IAM Service Role Object that will be used on instances within our compute environment to launch containers
 */
function generateBatchInstanceRole() {
    const batchInstanceRoleName = `BatchInstanceRole-${this.provider.getRegion()}-${this.provider.getStage()}-${this.provider.serverless.service.service}`.substring(0, 64);
    const batchInstanceManagementRoleTemplate = `
    {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "${batchInstanceRoleName}",
        "Path": "/",
        "AssumeRolePolicyDocument": {
          "Version": "2008-10-17",
          "Statement": [
            {
              "Sid": "",
              "Effect": "Allow",
              "Principal": {
                "Service": "ec2.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        },
        "ManagedPolicyArns": [
          "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
        ]
      }
    }
    `;

    const batchInstanceProfileTemplate = `
    {
      "Type": "AWS::IAM::InstanceProfile",
      "Properties": {
        "Path": "/",
        "Roles": [ 
          {
            "Ref": "${this.provider.naming.getBatchInstanceManagementRoleLogicalId()}"
          }
        ]
      }
    }
  `

    // Setup the JobQueue to push tasks to
    return {
        [this.provider.naming.getBatchInstanceManagementRoleLogicalId()]: JSON.parse(batchInstanceManagementRoleTemplate),
        [this.provider.naming.getBatchInstanceManagementProfileLogicalId()]: JSON.parse(batchInstanceProfileTemplate)
    };
}

/**
 * Generates the IAM Service Role Object that will be used to manage spot instances in the compute environment
 */
function generateBatchSpotFleetRole() {
    const batchSpotRoleName = `BatchSpotFleetRole-${this.provider.getRegion()}-${this.provider.getStage()}-${this.provider.serverless.service.service}`.substring(0, 64);
    const batchSpotRoleManagementTemplate = `
    {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "${batchSpotRoleName}",
        "Path": "/",
        "AssumeRolePolicyDocument": {  
           "Version":"2008-10-17",
           "Statement":[  
              {  
                 "Sid":"",
                 "Effect":"Allow",
                 "Principal":{  
                    "Service":"spotfleet.amazonaws.com"
                 },
                 "Action":"sts:AssumeRole"
              }
           ]
        },
        "ManagedPolicyArns": [
          "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
        ]
      }
    }
    `;

    return {
        [this.provider.naming.getBatchSpotFleetManagementRoleLogicalId()]: JSON.parse(batchSpotRoleManagementTemplate)
    }
}


/**
 * Generates the IAM Role that can be used by our lambda "schedule batch" functions
 */
function generateLambdaScheduleExecutionRole() {
    const lambdaScheduleExecutionRoleName = `BatchScheduleRole-${this.provider.getRegion()}-${this.provider.getStage()}-${this.provider.serverless.service.service}`.substring(0, 64);
    const lambdaScheduleExecutionRoleTemplate = `
    {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "${lambdaScheduleExecutionRoleName}",
        "Path": "/",
        "AssumeRolePolicyDocument": {
          "Version": "2008-10-17",
          "Statement": [
            {
              "Sid": "",
              "Effect": "Allow",
              "Principal": {
                "Service": "lambda.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        },
        "Policies": [
          {
            "PolicyName": "AWSBatchFullAccess",
            "PolicyDocument": {
              "Version": "2012-10-17",
              "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "batch:*",
                            "cloudwatch:GetMetricStatistics",
                            "ec2:DescribeSubnets",
                            "ec2:DescribeSecurityGroups",
                            "ec2:DescribeKeyPairs",
                            "ec2:DescribeVpcs",
                            "ec2:DescribeImages",
                            "ec2:DescribeLaunchTemplates",
                            "ec2:DescribeLaunchTemplateVersions",
                            "ecs:DescribeClusters",
                            "ecs:Describe*",
                            "ecs:List*",
                            "eks:DescribeCluster",
                            "eks:ListClusters",
                            "logs:Describe*",
                            "logs:Get*",
                            "logs:TestMetricFilter",
                            "logs:FilterLogEvents",
                            "iam:ListInstanceProfiles",
                            "iam:ListRoles"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "iam:PassRole"
                        ],
                        "Resource": [
                            "arn:aws:iam::*:role/AWSBatchServiceRole",
                            "arn:aws:iam::*:role/service-role/AWSBatchServiceRole",
                            "arn:aws:iam::*:role/ecsInstanceRole",
                            "arn:aws:iam::*:instance-profile/ecsInstanceRole",
                            "arn:aws:iam::*:role/iaws-ec2-spot-fleet-role",
                            "arn:aws:iam::*:role/aws-ec2-spot-fleet-role",
                            "arn:aws:iam::*:role/AWSBatchJobRole*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "iam:CreateServiceLinkedRole"
                        ],
                        "Resource": "arn:aws:iam::*:role/*Batch*",
                        "Condition": {
                            "StringEquals": {
                                "iam:AWSServiceName": "batch.amazonaws.com"
                            }
                        }
                    }
                ]
            }
          },
          {
            "PolicyName": "lambda-schedule-execution-policies",
            "PolicyDocument": {
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Action": [
                    "batch:SubmitJob"
                  ],
                  "Resource": [
                    "*"
                  ]
                }
              ]
            }
          },
          {
            "PolicyName": "lambda-schedule-execution-create-resource",
            "PolicyDocument": {
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Action": [
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups"
                  ],
                  "Resource": [
                    "*"
                  ]
                }
              ]
            }
          }
        ]
      }
    }
    `;

    // Setup the JobQueue to push tasks to
    return {
        [this.provider.naming.getLambdaScheduleExecutionRoleLogicalId()]: JSON.parse(lambdaScheduleExecutionRoleTemplate)
    };
}

/**
 * Generates the JobQueue object that we will submit tasks to
 */
function generateBatchJobQueue() {
    const batchJobQueueTemplate = `
    {
      "Type": "AWS::Batch::JobQueue",
      "Properties": {
        "Priority": 1,
        "ComputeEnvironmentOrder": [
          {
            "ComputeEnvironment": { "Ref": "${this.provider.naming.getBatchComputeEnvironmentLogicalId()}" },
            "Order": 1
          }
        ]
      }
    }
    `;

    const jobQueue = JSON.parse(batchJobQueueTemplate);

    // Apply tags from provider batch config to the JobQueue
    const batchTags = _.get(this.serverless.service, 'provider.batch.Tags', {});
    if (Object.keys(batchTags).length > 0) {
        jobQueue.Properties.Tags = batchTags;
    }

    return {
        [this.provider.naming.getBatchJobQueueLogicalId()]: jobQueue
    }
}

function isFargateComputeResources(computeResources) {
    return ["FARGATE", "FARGATE_SPOT"].includes(computeResources.Type);
}

/**
 * Generates the ComputeEnvironment Object that will be used to run tasks
 */
function generateBatchComputeEnvironment() {
    // Setup our compute environment


    const batchComputeResourceTemplate = `{}`;

    // Merge any overrides into our compute environment template
    const batchComputeResourceObject = _.merge(
        {},
        JSON.parse(batchComputeResourceTemplate),
        this.serverless.service.provider.batch
    )

    // Extract Tags before they end up in ComputeResources (Tags belong at the ComputeEnvironment level)
    const computeEnvironmentTags = batchComputeResourceObject.Tags || {};
    delete batchComputeResourceObject.Tags;

    // If we are a SPOT type, default the BigPercentage to 100% (always pay lowest market price)
    if (batchComputeResourceObject.hasOwnProperty("Type")
        && batchComputeResourceObject.Type == "SPOT"
        && !batchComputeResourceObject.hasOwnProperty("BidPercentage")) {

        batchComputeResourceObject["BidPercentage"] = 100;
    }

    /// Add securityGroupIds and subnets to the compute resource.
    if (this.serverless.service.provider.hasOwnProperty("batch")) {
        if (this.serverless.service.provider.batch.hasOwnProperty("SecurityGroupIds")) {
            batchComputeResourceObject["SecurityGroupIds"] = this.serverless.service.provider.batch.SecurityGroupIds;
        }
        if (this.serverless.service.provider.batch.hasOwnProperty("Subnets")) {
            batchComputeResourceObject["Subnets"] = this.serverless.service.provider.batch.Subnets;
        }
    }

    const batchComputeEnvironmentTemplate = `
      {
        "Type" : "AWS::Batch::ComputeEnvironment",
        "Properties" : {
          "ServiceRole" : { 
            "Fn::GetAtt": [
              "${this.provider.naming.getBatchServiceRoleLogicalId()}",
              "Arn" 
            ]
          },
          "Type" : "MANAGED",
          "ComputeResources": ${JSON.stringify(batchComputeResourceObject)}
        }
      }
    `;

    // Then merge the compute resource into the Compute Environment object
    const computeEnvironment = JSON.parse(batchComputeEnvironmentTemplate);

    // AWS Batch rejects ComputeEnvironment Tags for Fargate/Fargate Spot.
    if (
        Object.keys(computeEnvironmentTags).length > 0
        && !isFargateComputeResources(batchComputeResourceObject)
    ) {
        computeEnvironment.Properties.Tags = computeEnvironmentTags;
    }

    return {
        [this.provider.naming.getBatchComputeEnvironmentLogicalId()]: computeEnvironment
    }
}

/**
 * Adds the AWS Batch Compute Environment, Job Queue, and Job Definition to our cloud formation
 * template
 */
function generateAWSBatchTemplate() {
    this.serverless.cli.log("Generating AWS Batch");

    const newBatchServiceRoleObject = generateBatchServiceRole.bind(this)();
    const newBatchInstanceManagementRoleObject = generateBatchInstanceRole.bind(this)();
    const newBatchSpotFleetManagementObject = generateBatchSpotFleetRole.bind(this)();
    const newLambdaScheduleExecutionRoleObject = generateLambdaScheduleExecutionRole.bind(this)();
    const newBatchJobQueueObject = generateBatchJobQueue.bind(this)();
    const newBatchComputeEnvironmentObject = generateBatchComputeEnvironment.bind(this)();

    // Add it to our initial compiled cloud formation templates
    _.merge(
        this.serverless.service.provider.compiledCloudFormationTemplate.Resources,
        newBatchServiceRoleObject,
        newBatchInstanceManagementRoleObject,
        newBatchSpotFleetManagementObject,
        newLambdaScheduleExecutionRoleObject,
        newBatchJobQueueObject,
        newBatchComputeEnvironmentObject,
    );

    return BbPromise.resolve();
}

module.exports = {
    getBatchServiceRoleLogicalId,
    getBatchInstanceManagementRoleLogicalId,
    getBatchInstanceManagementProfileLogicalId,
    getBatchSpotFleetManagementRoleLogicalId,
    getLambdaScheduleExecutionRoleLogicalId,
    getBatchComputeEnvironmentLogicalId,
    getBatchJobQueueLogicalId,
    getBatchJobQueueName,
    validateAWSBatchServerlessConfig,
    generateAWSBatchTemplate
};
