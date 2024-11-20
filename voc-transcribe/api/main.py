import os
import re
import subprocess
import logging
import json

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from tempfile import NamedTemporaryFile, TemporaryDirectory

from typing import Union

from fastapi import FastAPI
import torch

logger = logging.getLogger("voc_transcribe")
logger.setLevel(logging.DEBUG)

app = FastAPI()

def _is_cuda_available():
    print(f"torch.cuda.is_available={torch.cuda.is_available()}")
    return torch.cuda.is_available()

def _get_transcript_file_path(output_path):
    return os.path.join(output_path, [f for f in os.listdir(output_path) if f.endswith('.json')][0])

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
        output_bucket_param = ssm_client.get_parameter(Name='/voc/OUTPUT_BUCKET', WithDecryption=False)
        output_bucket = output_bucket_param['Parameter']['Value']
        
        # Fetch OUTPUT_TRANSCRIBE_KEY from SSM
        output_transcribe_key_param = ssm_client.get_parameter(Name='/voc/OUTPUT_TRANSCRIBE_KEY', WithDecryption=False)
        output_transcribe_key = output_transcribe_key_param['Parameter']['Value']
        
        # Fetch HF_TOKEN from SSM
        hf_token_param = ssm_client.get_parameter(Name='/voc/HF_TOKEN', WithDecryption=True)
        hf_token = hf_token_param['Parameter']['Value']
        
        return output_bucket, output_transcribe_key, hf_token
    except ssm_client.exceptions.ParameterNotFound:
        raise RuntimeError("One or more SSM parameters not found.")
    except Exception as e:
        raise RuntimeError(str(e))

def _transcribe_by_whisper(input_path: str, local_output_path:str, HF_TOKEN: str):
    """
    Execute an OS level command to transcribe the audio file using the Whisper model.
    Parameters
    ----------
    input_path : str
        The path to the input audio file to be transcribed.
    local_output_path : str
        The path to the directory where the transcription output will be saved.
    HF_TOKEN : str
        The Hugging Face token used for authentication with the Whisper model.

    Returns
    -------
    str
        The path to the transcribed JSON file.

    Raises
    ------
    RuntimeError
        If the transcription process fails.
    """
    command = [
        "whisper-ctranslate2",
        input_path,
        "--model", "large-v3",
        "--language", "Cantonese",
        "--output_dir", local_output_path,
        "--output_format", "json",
    ]

    if _is_cuda_available():
        command.extend([
            "--device", "cuda",
            "--hf_token", HF_TOKEN
        ])
        
    result = subprocess.run(command, shell=False, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"Transcription failed: {result.stderr}")
    
    # Assuming the transcription tool returns JSON output
    transcript_path = os.path.join(local_output_path,f"{os.path.basename(input_path)}.json")
    
    return transcript_path

    

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/asr")
def transcribe_audio(bucket: str, key: str):
    try:
        # Get config from ssm
        OUTPUT_BUCKET, OUTPUT_TRANSCRIBE_KEY, HF_TOKEN = _get_ssm_parameters()

        # Download file 
        s3_client = boto3.client('s3')
        
        # Download the audio file to a temporary location
        with NamedTemporaryFile(delete=False) as temp_audio_file:
            s3_client.download_fileobj(bucket, key, temp_audio_file)
            temp_audio_file_path = temp_audio_file.name

        # generate a temporary folder
        with TemporaryDirectory() as temp_dir:
            local_output_path = temp_dir

        # Transcribe the audio file
        transcript_path = _transcribe_by_whisper(temp_audio_file_path, local_output_path, HF_TOKEN)

        # Read the JSON content from the transcript file
        with open(transcript_path, 'r', encoding='utf-8') as file:
            transcript_data = json.load(file)
                
        # Write the updated content back to the transcript file
        with open(transcript_path, 'w', encoding='utf-8') as file:
            json.dump(transcript_data, file, ensure_ascii=False, indent=4)

        # Upload the JSON file to the output S3 bucket
        output_key = f"{OUTPUT_TRANSCRIBE_KEY}/{os.path.basename(key)}.json"
        s3_client.upload_file(transcript_path, OUTPUT_BUCKET, output_key)

        return {"output_key": f"{output_key}"}

    except (NoCredentialsError, PartialCredentialsError):
        return {"error": "AWS credentials not found."}
    except Exception as e:
        return {"error": str(e)}
    except RuntimeError as e:
        return {"error": str(e)}


if __name__ == '__main__':
    event = {
        'bucket': "voc-input-144608043951",
        'key': 'test/CUST_00001_GUID_0000_AGENT_MelodyL_DT_2024-10-01T14-02-40_ChinaTelecomWong.wav',
    }

    transcribe_audio(event['bucket'], event['key'])
