
'use strict';
const process = require("process");
const { BatchClient, SubmitJobCommand } = require('@aws-sdk/client-batch')

const batch = new BatchClient({ region: process.env.AWS_REGION });


module.exports.schedule = (event, context, callback) => {

    if (process.env.EVENT_LOGGING_ENABLED === 'true') {
        console.log(`Received event: ${JSON.stringify(event, null, 2)}`);
    }

    const jobDefinition = process.env.JOB_DEFINITION_ARN;
    const jobQueue = process.env.JOB_QUEUE_ARN;

    let jobName = process.env.FUNCTION_NAME + '-' + event.target_name;

    if (event.chunks) {
        // Function to extract index from chunk name
        function getIndex(chunkName) {
            const match = chunkName.match(/part-(\d+)-/);
            return match ? parseInt(match[1], 10) : null;
        }

        // Extract indices
        const indices = event.chunks.map(getIndex).filter(index => index !== null);

        // Determine min and max indices
        if (indices.length === 1) {
            // If there's only one index, append it as [index]
            jobName += `-${indices[0]}`;
        } else if (indices.length > 1) {
            // Multiple indices, calculate min and max
            const minIndex = Math.min(...indices);
            const maxIndex = Math.max(...indices);
            jobName += `-${minIndex}_${maxIndex}`;
        }
    }
    

    console.log(`Submitting job: ${jobName} with jobDefinition: ${jobDefinition} to queue: ${jobQueue}`);

    // delete headers from the event, we are exceeding the 8092 limit for container overrides
    delete event.headers;
    delete event.multiValueHeaders;

    let params = {
        jobDefinition: jobDefinition,
        jobName: jobName,
        jobQueue: jobQueue,
        parameters: {
            event: JSON.stringify(event)
        },
        containerOverrides: {
            environment: [
                { name: "AWS_LAMBDA_FUNCTION_NAME", value: process.env.AWS_LAMBDA_FUNCTION_NAME },
                { name: "AWS_LAMBDA_FUNCTION_VERSION", value: process.env.AWS_LAMBDA_FUNCTION_VERSION },
                { name: "AWS_REQUEST_ID", value: context.awsRequestId }
            ]
        },
        propagateTags: true,
        tags: {
            "target": event.target_name,
            "team": "data",
            "project": "data-ml-serverless-stack",
            "tagger": "plugin"
        }
    };

    const command = new SubmitJobCommand(params);

    console.log(`Submitting job: ${JSON.stringify(params, null, 2)}`);

    batch.send(new SubmitJobCommand(params), function (err, data) {
        let response;

        const jsonHeaders = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',        // Required for CORS support to work
            'Access-Control-Allow-Credentials': true   // Required for cookies, authorization headers with HTTPS 
        }

        if (err) {
            console.log(`Error submitting job: ${err}`, err.stack);
            response = {
                statusCode: 500,
                headers: jsonHeaders,
                body: JSON.stringify({
                    'error': err
                })
            }
        }
        else {
            console.log(`Submitted job: ${JSON.stringify(data, null, 2)}`);
            response = {
                statusCode: 200,
                headers: jsonHeaders,
                input: event,
                body: data
            }
        }

        callback(null, response);
    });

}