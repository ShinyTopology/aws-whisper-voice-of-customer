import re
import os
import requests
import boto3

def _get_ssm_parameters():
    """
    Fetches necessary parameters from AWS Systems Manager (SSM) Parameter Store.

    This function retrieves the following parameters:
    - OUTPUT_BUCKET: The S3 bucket where the transcribed output will be stored.
    - OUTPUT_TRANSCRIBE_KEY: The key prefix under which the transcribed output will be stored.
    - HF_TOKEN: The Hugging Face token used for authentication with the Whisper model.

    Returns
    -------
    tuple
        A tuple containing the values of OUTPUT_BUCKET, OUTPUT_TRANSCRIBE_KEY, and HF_TOKEN.

    Raises
    ------
    RuntimeError
        If one or more SSM parameters are not found or any other error occurs during the fetch operation.
    """
    ssm_client = boto3.client('ssm')
    try:
        # Fetch OUTPUT_BUCKET from SSM
        parameters = ssm_client.get_parameters(
            Names=[
                '/voc/TRANSCRIPTION_API_URL'
            ],
            WithDecryption=False
        )

        param_dict = {p['Name']: p['Value'] for p in parameters['Parameters']}
        
        output_parameters = {
            'transcription_api_url': param_dict.get('/voc/TRANSCRIPTION_API_URL', "http://11.0.0.125:8000/asr")
        }
        
        return output_parameters
    except ssm_client.exceptions.ParameterNotFound:
        raise RuntimeError("One or more SSM parameters not found.")
    except Exception as e:
        raise RuntimeError(str(e))

def lambda_handler(event, context):
    """Sample Lambda function which mocks the operation of checking the current price 
    of a stock.

    For demonstration purposes this Lambda function simply returns 
    a random integer between 0 and 100 as the stock price.

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: Object containing the current price of the stock
    """

    # Extract the input parameters from the event
    bucket = event['bucket']
    key = event['key']

    output_parameters = _get_ssm_parameters()

    url = output_parameters['transcription_api_url']
    # url = f"http://11.0.0.125:8000/asr"
    # url = f"http://localhost:8000/asr"
    
    try:
        response = requests.request(
            method="POST",
            url=url,
            params={
                "bucket" : bucket,
                "key" : key
            },
            timeout=100
        )

        print(response.json())
        print(type(response.json()))

        return {
            "bucket" : bucket,
            "key" : key,
            "output_key": response.json()['output_key']
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return {
            'error': f"Error making request: {e}"
        }


if __name__ == '__main__':
    event = {
        'bucket': "voc-input-144608043951",
        'key': 'test/CUST_00001_GUID_0000_AGENT_MelodyL_DT_2024-10-01T14-02-40_ChinaTelecomWong.wav',
    }

    output = lambda_handler(event, None)
    print(output)