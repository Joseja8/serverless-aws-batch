
'use strict';
const process = require("process");
const { BatchClient, SubmitJobCommand } = require('@aws-sdk/client-batch')

const batch = new BatchClient({ region: process.env.AWS_REGION });


module.exports.schedule = (event, context, callback) => {

    if (process.env.EVENT_LOGGING_ENABLED === 'true') {
        console.log(`Received event: ${JSON.stringify(event, null, 2)}`);
    }

    let jobName = process.env.FUNCTION_NAME;

    const jobDefinition = process.env.JOB_DEFINITION_ARN;
    const jobQueue = process.env.JOB_QUEUE_ARN;

    if (event.ea_name) {
        jobName = jobName + '-' + event.ea_name;
    }


    // Function to extract index from chunk name.
    // Chunk names are expected to be in the format "part-<index>-<other_info>". E.g., "part-00042-0bd8895b-67b9-4b55-bb18-0d024a55f5d7-c000.snappy.parquet" -> index: 42.
    function getIndex(chunkName) {
        const match = chunkName.match(/part-(\d+)-/);
        return match ? parseInt(match[1], 10) : null;
    }


    if (event.chunks) {
        // Make chunks an array for easier processing. This handles both single string and array inputs.
        const chunks = [].concat(event.chunks);

        // If there's only one chunk, append its index. If multiple, append min and max indices.
        if (chunks.length === 1) {
            jobName += `-${getIndex(chunks[0])}`;
        } else if (chunks.length > 1) {
            const minIndex = Math.min(...chunks.map(getIndex));
            const maxIndex = Math.max(...chunks.map(getIndex));
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
            "team": "data",
            "project": "data-ml-serverless-stack",
            "step": event.step || "unknown",
            "target": event.ea_name || "unknown",
            "env": event.env || "unknown",
            "tagger": "aws-batch-plugin"
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