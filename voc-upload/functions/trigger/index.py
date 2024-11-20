import json
import os
import boto3

stepfunctions = boto3.client('stepfunctions')

def handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Prepare input for Step Function
    input_data = {
        "bucket": bucket,
        "key": key
    }
    
    # Start Step Function execution
    response = stepfunctions.start_execution(
        stateMachineArn=os.environ['STATE_MACHINE_ARN'],
        input=json.dumps(input_data)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Step Function execution started',
            'executionArn': response['executionArn']
        })
    } 

if __name__ == '__main__':
    event = {
        "Records": [
            {
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "eca58aa9-dd2b-4405-94d5-d5fba7fd0a16",
                    "bucket": {
                        "name": "voc-input-144608043951",
                        "ownerIdentity": {
                            "principalId": "A39I0T5T4Z0PZJ"
                        },
                        "arn": "arn:aws:s3:::ajk-call-analytics-demo"
                    },
                    "object": {
                        # "key": "originalTranscripts/redacted-Auto0_GUID_000_AGENT_ChrisL_DT_2022-03-19T06-01-22_Mono.wav.json",
                        # "key": "originalTranscripts/Auto1_GUID_001_AGENT_AndrewK_DT2022-03-20T07-55-51.wav.json",
                        "key": "inputAudio/CUST_00001_GUID_0000_AGENT_MelodyL_DT_2024-10-01T14-02-40_ChinaTelecomWong.wav",
                        "size": 963023,
                        "eTag": "8588ee73ae57d72c072f4bc401627724",
                        "sequencer": "005E99B1F567D61004"
                    }
                }
            }
        ]
    }
        
    handler(event, None)

