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

const noOfMessagesToprocessPerMinute = 4;


// main function 
exports.handler = (event, context, callback) => {
    let count = 0;
    // while (count != noOfMessagesToprocessPerMinute) {
    //     // process the queue 

    // }

    // no of messages in queue;
    var params = {
        QueueUrl: hqueueURL,
        /* required */
        AttributeNames: ["ApproximateNumberOfMessages"]
    };
    sqs.getQueueAttributes(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(data); // successful response
            let noofmessages = (data.Attributes.ApproximateNumberOfMessages)
            if (noofmessages > 0) {
                //  while (noofmessages != 0) {
                let params = {
                    QueueUrl: hqueueURL,
                    /* required */
                    MaxNumberOfMessages: noOfMessagesToprocessPerMinute,
                };

                sqs.receiveMessage(params, function(err, data) {
                    if (err) console.log(err, err.stack); // an error occurred
                    else console.log(data); // successful response
                    let messagesArray = data.Messages;

                    messagesArray.forEach((amessage) => {
                        let ReceiptHandleId = amessage.ReceiptHandle;
                        callback(null, JSON.stringify(messagesArray));
                        documentClient.put({
                                TableName: 'processed',
                                Item: {
                                    epoch: (new Date().getTime()).toString(),
                                    timestamp: (new Date()).toString(),
                                    status: "processed"
                                    // reciepthandleId: ReceiptHandleId
                                }
                            },
                            function(err, data) {
                                if (err) console.log(err);
                                else { callback(null, JSON.stringify(data)); }
                            });

                        let delparams = {
                            QueueUrl: hqueueURL,
                            /* required */
                            ReceiptHandle: ReceiptHandleId /* required */
                        };
                        
                        sqs.deleteMessage(delparams, function(err, data) {
                            if (err) console.log(err, err.stack); // an error occurred
                            else console.log(data); // successful response
                        });

                    });

                });

                // }

            }

        }
    });

    // read messages  from queue. 

};
