// Declarations
const AWS = require('aws-sdk');

AWS.config.update({
    region: "ap-south-1"
    // accessKeyId: "",
    // secretAccessKey: ""
});

const s3 = new AWS.S3();
// const dynamodb = new AWS.DynamoDB();
const documentClient = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();

const hqueueURL = "https://sqs.ap-south-1.amazonaws.com/805746249177/hqueue";
const lqueueURL = "https://sqs.ap-south-1.amazonaws.com/805746249177/lqueue";
const TableName = "log";

async function gets3Object(src_bkt, src_key) {
    try {
        const params = {
            Bucket: src_bkt,
            Key: src_key
        };

        const item = (await s3.getObject(params).promise()).Body;
        // console.log(JSON.stringify(data));
        console.log("buffer : " + (item));
        return JSON.parse(item);
    } catch (e) {
        throw new Error(`Could not retrieve file from S3: ${e.message}`);
    }
}

async function saveThisToTableServerless(message, operation) {
    const params = {
        TableName: TableName,
        Item: {
            timestamp: (new Date().getTime()).toString(),
            status: "pending",
            message: message,
            operation: operation
        }
    };
    console.log("Executing dynamo put " + JSON.stringify(params));

    try {
        const dynamo_promise = await documentClient.put(params).promise();
        console.log("success : " + JSON.stringify(dynamo_promise));
        return true;
    } catch (error) {
        console.log(`failed to execute: ${error.stack}`);
        return false;
    }
}

async function sendToQueueAccToPriority(jsonItem) {
    const q_priority = jsonItem.priority.toString();
    const q_message = jsonItem.message.toString();

    let queue_params = {
        MessageAttributes: {
            "priority": {
                DataType: "String",
                StringValue: q_priority
            },
            "message": {
                DataType: "String",
                StringValue: q_message
            }
        },
        MessageBody: `{  
                "priority": ${q_priority},
                "message": ${q_message}
                }`,
        // QueueUrl: hqueueURL
    };

    if (q_priority === '1')
        queue_params['QueueUrl'] = hqueueURL;
    else if (q_priority === '2')
        queue_params['QueueUrl'] = lqueueURL;
    else {
        console.log("Priority does not match");
        return false;
    }

    console.log("queue Params : " + JSON.stringify(queue_params));

    try {
        const queue_promise = await sqs.sendMessage(queue_params).promise();
        console.log("success : " + JSON.stringify(queue_promise.MessageId));
        return true;
    } catch (error) {
        console.log(`failed to execute: ${error.stack}`);
        return false;
    }

}

exports.handler  = async (event, context, callback) => {
    // TODO implement

// debug static code 
    // let src_bkt = 'lambdatest-97559806';  // event.Records[0].s3.bucket.name;
    // let src_key = 'requests/file1.json';  //event.Records[0].s3.object.key;

// final dyanmic code
    let src_bkt =   event.Records[0].s3.bucket.name;
    let src_key =   event.Records[0].s3.object.key;


    let s3itemjson = await gets3Object(src_bkt, src_key);
    console.log(s3itemjson.message);
    if (sendToQueueAccToPriority(s3itemjson)) {
        console.log("execution success");
        await saveThisToTableServerless("successful execution", "lambda exec");
    } else {
        console.log("failed execution: some error");
        await saveThisToTableServerless("Failed Execution", "Lambda exec");
    }
};
