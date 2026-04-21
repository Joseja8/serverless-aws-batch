
'use strict';
const process = require("process");
const { BatchClient, SubmitJobCommand } = require('@aws-sdk/client-batch')

const batch = new BatchClient({ region: process.env.AWS_REGION });


module.exports.schedule = (event, context, callback) => {

    if (process.env.EVENT_LOGGING_ENABLED === 'true') {
        console.log(`Received event: ${JSON.stringify(event, null, 2)}`);
    }

    const MAX_JOB_NAME_LENGTH = 128;

    // Build the base: strip trailing "Lambda" suffix from the function name.
    let baseName = process.env.FUNCTION_NAME.replace(/Lambda$/, '');

    const jobDefinition = process.env.JOB_DEFINITION_ARN;
    const jobQueue = process.env.JOB_QUEUE_ARN;

    // Optional name segments (ea_name, chunk indices).
    let nameSuffix = '';

    if (event.ea_name) {
        nameSuffix += '-' + event.ea_name;
    }

    // Function to extract index from chunk name.
    // Chunk names are expected to be in the format "part-<index>-<other_info>". E.g., "part-00042-0bd8895b-67b9-4b55-bb18-0d024a55f5d7-c000.snappy.parquet" -> index: 42.
    function getIndex(chunkName) {
        const match = chunkName.match(/part-(\d+)-/);
        return match ? parseInt(match[1], 10) : null;
    }

    if (event.chunks) {
        const chunks = [].concat(event.chunks);
        if (chunks.length === 1) {
            nameSuffix += `-${getIndex(chunks[0])}`;
        } else if (chunks.length > 1) {
            const minIndex = Math.min(...chunks.map(getIndex));
            const maxIndex = Math.max(...chunks.map(getIndex));
            nameSuffix += `-${minIndex}_${maxIndex}`;
        }
    }

    // Derive ymd (YYYYMMDD) from event.ymd ("YYYY-MM-DD") or event.ymdh ("YYYYMMDDHH"), fallback to today.
    let ymd;
    if (event.ymd) {
        ymd = event.ymd.replace(/-/g, '');
    } else if (event.ymdh) {
        ymd = event.ymdh.slice(0, 8);
    } else {
        const d = new Date();
        ymd = d.getFullYear().toString()
            + String(d.getMonth() + 1).padStart(2, '0')
            + String(d.getDate()).padStart(2, '0');
    }

    const env = process.env.TAG_ENV || event.env || 'unknown';
    const suffix = `-batch-${env}-${ymd}`;

    // Sanitise and truncate so the full name stays within the Batch 128-char limit.
    const sanitised = (baseName + nameSuffix).replace(/[^A-Za-z0-9_-]/g, '').slice(0, MAX_JOB_NAME_LENGTH - suffix.length);
    let jobName = sanitised + suffix;


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
        tags: Object.fromEntries(
            Object.entries({
                "team": process.env.TAG_TEAM,
                "project": process.env.TAG_PROJECT,
                "env": process.env.TAG_ENV || event.env,
                "pipeline": process.env.TAG_PIPELINE,
                "lambda": process.env.TAG_LAMBDA || process.env.FUNCTION_NAME
            }).filter(([_, v]) => v)
        )
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