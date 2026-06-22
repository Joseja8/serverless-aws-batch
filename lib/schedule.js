"use strict";
const process = require("process");
const { BatchClient, SubmitJobCommand } = require("@aws-sdk/client-batch");

const batch = new BatchClient({ region: process.env.AWS_REGION });

const MAX_JOB_NAME_LENGTH = 128;
const MAX_TAG_VALUE_LENGTH = 256;

function parseJsonEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  try {
    return JSON.parse(raw);
  } catch (err) {
    console.warn(`Ignoring invalid ${name}: ${err.message}`);
    return fallback;
  }
}

function getEventPathValue(event, path) {
  if (!path || typeof path !== "string") {
    return undefined;
  }

  const normalized = path.replace(/^\$\.?/, "").replace(/\[(\d+)\]/g, ".$1");

  if (!normalized) {
    return event;
  }

  return normalized
    .split(".")
    .filter(Boolean)
    .reduce((value, key) => {
      if (value === undefined || value === null) {
        return undefined;
      }
      return value[key];
    }, event);
}

function resolveValue(spec, event) {
  if (spec && typeof spec === "object" && !Array.isArray(spec)) {
    if (Object.prototype.hasOwnProperty.call(spec, "path")) {
      return getEventPathValue(event, spec.path);
    }
    if (Object.prototype.hasOwnProperty.call(spec, "value")) {
      return spec.value;
    }
    if (Object.prototype.hasOwnProperty.call(spec, "env")) {
      return process.env[spec.env];
    }
  }

  if (typeof spec === "string") {
    return getEventPathValue(event, spec);
  }

  return spec;
}

function stringifyValue(value) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (Array.isArray(value)) {
    return value.map(stringifyValue).filter(Boolean).join("_");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function resolveExecutionTags(event) {
  const configuredTags = parseJsonEnv("BATCH_EXECUTION_TAGS", {});
  if (
    !configuredTags ||
    typeof configuredTags !== "object" ||
    Array.isArray(configuredTags)
  ) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(configuredTags)
      .map(([key, spec]) => [key, stringifyValue(resolveValue(spec, event))])
      .filter(([key, value]) => key && value !== undefined)
      .map(([key, value]) => [key, value.slice(0, MAX_TAG_VALUE_LENGTH)]),
  );
}

function resolveJobNameSuffixes(event) {
  const configuredSuffixes = parseJsonEnv("BATCH_JOB_NAME_SUFFIXES", []);
  const suffixes = Array.isArray(configuredSuffixes)
    ? configuredSuffixes
    : [configuredSuffixes];

  return suffixes
    .map((spec) => stringifyValue(resolveValue(spec, event)))
    .filter(Boolean)
    .map((segment) => `-${segment}`)
    .join("");
}

module.exports.schedule = (event, context, callback) => {
  if (process.env.EVENT_LOGGING_ENABLED === "true") {
    console.log(`Received event: ${JSON.stringify(event, null, 2)}`);
  }

  // Build the base: strip trailing "Lambda" suffix from the function name.
  let baseName = process.env.FUNCTION_NAME.replace(/Lambda$/, "");

  const jobDefinition = process.env.JOB_DEFINITION_ARN;
  const jobQueue = process.env.JOB_QUEUE_ARN;

  const nameSuffix = resolveJobNameSuffixes(event);

  // Derive ymd (YYYYMMDD) from event.ymd ("YYYY-MM-DD") or event.ymdh ("YYYYMMDDHH"), fallback to today.
  let ymd;
  if (event.ymd) {
    ymd = event.ymd.replace(/-/g, "");
  } else if (event.ymdh) {
    ymd = event.ymdh.slice(0, 8);
  } else {
    const d = new Date();
    ymd =
      d.getFullYear().toString() +
      String(d.getMonth() + 1).padStart(2, "0") +
      String(d.getDate()).padStart(2, "0");
  }

  const env = process.env.TAG_ENV || event.env || "unknown";
  const suffix = `-batch-${env}-${ymd}`;

  // Sanitise and truncate so the full name stays within the Batch 128-char limit.
  const sanitised = (baseName + nameSuffix)
    .replace(/[^A-Za-z0-9_-]/g, "")
    .slice(0, MAX_JOB_NAME_LENGTH - suffix.length);
  let jobName = sanitised + suffix;

  console.log(
    `Submitting job: ${jobName} with jobDefinition: ${jobDefinition} to queue: ${jobQueue}`,
  );

  // delete headers from the event, we are exceeding the 8092 limit for container overrides
  delete event.headers;
  delete event.multiValueHeaders;

  let params = {
    jobDefinition: jobDefinition,
    jobName: jobName,
    jobQueue: jobQueue,
    parameters: {
      event: JSON.stringify(event),
    },
    containerOverrides: {
      environment: [
        {
          name: "AWS_LAMBDA_FUNCTION_NAME",
          value: process.env.AWS_LAMBDA_FUNCTION_NAME,
        },
        {
          name: "AWS_LAMBDA_FUNCTION_VERSION",
          value: process.env.AWS_LAMBDA_FUNCTION_VERSION,
        },
        { name: "AWS_REQUEST_ID", value: context.awsRequestId },
      ],
    },
    propagateTags: true,
    tags: Object.fromEntries(
      Object.entries({
        team: process.env.TAG_TEAM,
        project: process.env.TAG_PROJECT,
        env: process.env.TAG_ENV || event.env,
        pipeline: process.env.TAG_PIPELINE,
        lambda: process.env.TAG_LAMBDA || process.env.FUNCTION_NAME,
        ...resolveExecutionTags(event),
      }).filter(([_, v]) => v),
    ),
  };

  const command = new SubmitJobCommand(params);

  console.log(`Submitting job: ${JSON.stringify(params, null, 2)}`);

  batch.send(command, function (err, data) {
    let response;

    const jsonHeaders = {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true, // Required for cookies, authorization headers with HTTPS
    };

    if (err) {
      console.log(`Error submitting job: ${err}`, err.stack);
      response = {
        statusCode: 500,
        headers: jsonHeaders,
        body: JSON.stringify({
          error: err,
        }),
      };
    } else {
      console.log(`Submitted job: ${JSON.stringify(data, null, 2)}`);
      response = {
        statusCode: 200,
        headers: jsonHeaders,
        input: event,
        body: data,
      };
    }

    callback(null, response);
  });
};
