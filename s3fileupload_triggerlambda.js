const AWS = require('aws-sdk');

AWS.config.update({
    region: "ap-south-1",
    accessKeyId: "AKIAI3HH2XG6DK2GA5SQ",
    secretAccessKey: "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"
});

const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB();
const documentClient = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();

const hqueueURL = "https://sqs.ap-south-1.amazonaws.com/776773931318/hqueue";
const lqueueURL = "https://sqs.ap-south-1.amazonaws.com/776773931318/lqueue";


// main function 


exports.handler = (event, context, callback) => {
    // s3 code 

    var src_bkt = event.Records[0].s3.bucket.name;
    var src_key = event.Records[0].s3.object.key;

    // Retrieve the object
    s3.getObject({
        Bucket: src_bkt,
        Key: src_key
    }, function(err, data) {
        if (err) {
            console.log(err, err.stack);
            callback(err);
        }
        else {
            let jsonObject = data.Body.toString('ascii');
            let jsonuse = JSON.parse(jsonObject);
            let queueparams = "";
            // console.log("Raw text:\n" + data.Body.toString('ascii'));
            // callback(null, JSON.stringify(data));
            let qpriorty = ((jsonuse.priority).toString());

            if (qpriorty === "1") {
                queueparams = {

                    MessageAttributes: {
                        "priority": {
                            DataType: "String",
                            StringValue: qpriorty
                        },
                        "message": {
                            DataType: "String",
                            StringValue: ((jsonuse.message).toString())
                        }
                    },
                    MessageBody: `{  
                "priority": ${qpriorty},
                "message": ${((jsonuse.message).toString())}
                }`,
                    QueueUrl: hqueueURL
                };
                sqs.sendMessage(queueparams, function(err, data) {
                    if (err) console.log(err, err.stack); // an error occurred
                    else console.log(data); // successful response
                });

            }
            else if (qpriorty === "2") {

                queueparams = {

                    MessageAttributes: {
                        "priority": {
                            DataType: "String",
                            StringValue: qpriorty
                        },
                        "message": {
                            DataType: "String",
                            StringValue: ((jsonuse.message).toString())
                        }
                    },
                    MessageBody: `{  
                "priority": ${qpriorty},
                "message": ${((jsonuse.message).toString())}
                }`,
                    QueueUrl: lqueueURL
                };
                sqs.sendMessage(queueparams, function(err, data) {
                    if (err) console.log(err, err.stack); // an error occurred
                    else console.log(data); // successful response
                });
            }

            documentClient.put({
                    TableName: 'log',
                    Item: {
                        epoch: (new Date().getTime()).toString(),
                        timestamp: "For s3 data",
                        status: "pending",
                        quepriority: qpriorty
                    }
                },
                function(err, data) {
                    if (err) console.log(err);
                    else { callback(null, JSON.stringify(data)); }
                });
        }
    });

};
