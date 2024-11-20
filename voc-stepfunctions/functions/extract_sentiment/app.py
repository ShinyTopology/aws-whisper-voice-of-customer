from datetime import datetime
from random import randint
from uuid import uuid4
import os
import re
import boto3
import json
import logging
import time
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
brclient = boto3.client("bedrock-runtime")
braclient = boto3.client("bedrock-agent")
athena_client = boto3.client('athena')



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
                '/voc/OUTPUT_BUCKET',
                '/voc/GLUE_DB',
                '/voc/GLUE_TABLE_PROCESSED_TRANSCRIPTION',
                '/voc/EXTRACT_ENTITY_PROMPT_IDENTIFIER',
                '/voc/EXTRACT_ENTITY_PROMPT_VERSION',
                '/voc/EXTRACT_ENTITY_PROMPT_VARIANT'
            ],
            WithDecryption=False
        )

        param_dict = {p['Name']: p['Value'] for p in parameters['Parameters']}
        
        output_parameters = {
            'output_bucket': param_dict['/voc/OUTPUT_BUCKET'],
            'glue_db': param_dict.get('/voc/GLUE_DB', 'voc_db'),
            'glue_table_processed_transcription': param_dict.get('/voc/GLUE_TABLE_PROCESSED_TRANSCRIPTION', 'voc_processed_transcription'),
            'extract_entity_prompt_identifier': param_dict.get('/voc/EXTRACT_ENTITY_PROMPT_IDENTIFIER', 'CI94EO0SIQ'),
            'extract_entity_prompt_version': param_dict.get('/voc/EXTRACT_ENTITY_PROMPT_VERSION', '6'),
            'extract_entity_prompt_variant': param_dict.get('/voc/EXTRACT_ENTITY_PROMPT_VARIANT', 'variantOne')
        }
        
        return output_parameters
    except ssm_client.exceptions.ParameterNotFound:
        raise RuntimeError("One or more SSM parameters not found.")
    except Exception as e:
        raise RuntimeError(str(e))


def extract_info_from_filename(key: str):

    filename = os.path.basename(key)
    
    pattern = r"CUST_(?P<cust>\w+)_GUID_(?P<guid>\w+)_AGENT_(?P<agent>\w+)_DT_(?P<conversationtime>[\d-]+T[\d-]+)_(?P<remark>[\w]+)\.wav.json"
    match = re.match(pattern, filename)
    
    if match:
        fileinfo = match.groupdict()
        fileinfo['filename'] = filename
        return fileinfo.get('cust'), fileinfo.get('guid'), fileinfo.get('agent'), fileinfo.get('conversationtime'), fileinfo.get('remark')
        # {
        #     "cust": "CUST_00001",
        #     "guid": "GUID_0000",
        #     "agent": "MelodyL",
        #     "conversationtime": "2024-10-01T14-02-40",
        #     "remarks": "ChinaTelecomWong"
        # }
    else:
        raise ValueError(f"Filename {filename} does not match the expected pattern")

def get_prompt_template(prompt_identifier = '0Z4YY8K2G6', prompt_version = 2) -> tuple[str, str, dict]:

    response = braclient.get_prompt(
        promptIdentifier=prompt_identifier,
        promptVersion=prompt_version
    )
    
    """
    {
    'defaultVariant': 'variantOne',
    'id': '0Z4YY8K2G6',
    'name': 'voc_generate_dml_processed_transcription',
    'updatedAt': datetime.datetime(2024, 11, 13, 8, 43, 45, 534248, tzinfo=tzutc()),
    'variants': [{'additionalModelRequestFields': {'top_k': 250.0},
    'inferenceConfiguration': {'text': {'maxTokens': 2000,
        'stopSequences': ['\n\nHuman:'],
        'temperature': 1.0,
        'topP': 0.9990000128746033}},
    'modelId': 'anthropic.claude-3-sonnet-20240229-v1:0',
    'name': 'variantOne',
    'templateConfiguration': {'text': {'inputVariables': [],
        'text': 'You are an SQL expert who is good at writing Amazon Athena DML based on provided schema, and the direction.\nWrite INSERT statements that insert records into the specified Amazon Athena iceberg table. Follow the following instructions. write one statement only\n- call_nature should be :\n技術支援\n帳户查詢\n月結單內容\n續約優惠\n合約到期日查詢\n續約優惠\n報失電話\n申請延遲繳費\n解除通話限制\n操縱遙控轉駁\n- agent should be : ChrisK,MelodyL,PeterC\n- cust should be integer between 1 to 10\n- conversation_time should be a datatime value in yyyy-mm-dd hh:mm:ss format in 2024 october\n- conversation_location should be 1 of the 18 districts in Hong Kong. Only include one district.\n- language_code should be : yue / en_us\n- related_products should be one of the following:\n光纖寬頻\n寬頻上網\n手機服務\n寬頻及家居服務\n儲值卡服務\n應用程式及服務\niPhone手機及月費計劃詳情\n- categories_detected_text should be one of the following :\n技術支援\n帳户查詢\n月結單內容\n續約優惠\n合約到期日查詢\n續約優惠\n報失電話\n申請延遲繳費\n解除通話限制\n操縱遙控轉駁\n- customer_sentiment_socre should be float between -5 to 5\n- agent_sentiment_socre should be float between -5 to 5\n- sys_s3_path_string should with prefix s3://voc-output-144608043951/TranscribedOutput/\n- Include all columns\n- conversation_duration should be between 10 to 240\n\n<DDL>\nCREATE TABLE voc_db.voc_processed_transcription (\n  guid string,\n  file_name string,\n  call_nature string, \n  summary string,\n  agent string,\n  customer_id int,\n  conversation_time timestamp,\n  conversation_duration double,\n  conversation_location string,\n  language_code string,\n  related_products ARRAY<string>,\n  related_location ARRAY<string>,\n  action_items_detected_text string,\n  issues_detected_text string,\n  outcomes_detected_text string,\n  categories_detected_text string,\n  custom_entities ARRAY<string>,\n  categories_detected ARRAY<string>,\n  customer_sentiment_score double,\n  agent_sentiment_score double,\n  customer_total_time_secs double,\n  agent_total_time_secs double,\n  raw_transcript_text string,\n  raw_segments ARRAY<STRUCT<id: int,seek: int,start: double,end: double,text: string,tokens: ARRAY<bigint>,temperature: double,avg_logprob: double,compression_ratio: double,no_speech_prob: double,words: ARRAY<string>>>,\n  sys_s3_path string,\n  sys_process_time timestamp\n)\n</DDL>'}},
    'templateType': 'TEXT'}],
    'version': '2'
    }
    """
    
    # get default variant name
    default_variant_name = response['defaultVariant']


    # Get default variant content
    default_variant = next((variant for variant in response['variants'] 
                            if variant['name'] == default_variant_name) , None)

    user_prompt = default_variant['templateConfiguration']['chat']['messages'][0]['content'][0]['text']
    system_prompt = default_variant['templateConfiguration']['chat']['system'][0]['text']

    PROMPT_TEMPALTE = f"""{system_prompt}
{user_prompt}"""

    return PROMPT_TEMPALTE, default_variant['modelId'], default_variant['inferenceConfiguration']

def extract_entity_using_llm(transcript_filename : str, transcript_data: dict, prompt_identifier: str, prompt_version: int, prompt_variant: str):
    """
    Extract sentiment and other entities using bedrock

    transcript_data: dict
    {
        "text": " 歡迎致電中國移動香港客戶服務熱線 請等一等 我們的客戶服務主任會盡快接聽您的電話 為確保客戶服務質素 以下的兌換內容可能會被錄音 您好,中國移動香港接電話事業姓劉 請問有些甚麼可以幫到您 我姓胡,有個電話號碼 因為剛剛尋錢被人cut了 因為遲了交電話費 我現在已經到門市剛剛付了錢 那裡有488元 因為我現在趕著回大陸 需要用到個電話 看看可不可以即時開通 這樣情況 明白的 請問胡先生的電話號碼是多少,不好意思 60673031 登記人姓黃 Sorry,我想請問登記記住 你是不是在這裡 我有他的資料,我可不可以告訴你 不好意思,胡先生 因為首先如果有個號碼 你要開機的話 要登記記住聯絡我們才可以做得到 那我姓黃的,你好 不好意思 請問60673031是不是黃先生本人登記 是,沒錯 但是,Sorry,是小姐 你好,我姓黃的,你好 OK,不好意思,黃小姐 我想請問60673031是本人登記,是嗎 是,沒錯 謝謝,保證真的要獲利益 所以和黃小姐先兌換資料 是 請問黃小姐的全名怎麼稱呼 黃玉珍 謝謝,請問身份證號碼是多少 244047410 謝謝,請問生日是幾月幾號 11月16日 謝謝,黃小姐 謝謝 這邊我幫黃小姐查班 號碼開機方面幫你看看 是 看看,OK,幫你號碼開機 半個小時之後請你關機開機就可以用得到 可不可以即時搞得到 因為我現在已經差不多過班了 明白,不好意思 因為系統過機都要時間 這邊我已經幫你下單了 請你轉頭關機開機 好,謝謝你 不用客氣 請問還有其他方便嗎,黃小姐? 沒有,你只有我黃小姐,謝謝你 怎麼稱呼你 不用客氣,我姓盧的 盧小姐,謝謝你 不用客氣 拜拜 謝謝黃小姐的電話查詢 希望無意服務 黃小姐,拜拜 拜拜",
        "segments": [
            {
                "id": 1,
                "seek": 2900,
                "start": 0.0,
                "end": 8.0,
                "text": " 歡迎致電中國移動香港客戶服務熱線",
                "tokens": [
                    50365, 220, 21315, 6784, 112, 20545, 21854, 8204, 119, 12572, 27141, 32316, 1486, 114, 27408, 39517, 32745, 31699, 50765
                ],
                "temperature": 0.0,
                "avg_logprob": -0.2264921152376914,
                "compression_ratio": 1.2801932367149758,
                "no_speech_prob": 0.58056640625,
                "words": null
            },
            {
                "id": 2,
                "seek": 2900,
                "start": 8.0,
                "end": 13.0,
                "text": " 請等一等",
                "tokens": [
                    50765, 220, 16302, 10187, 2257, 10187, 51015
                ],
                "temperature": 0.0,
                "avg_logprob": -0.2264921152376914,
                "compression_ratio": 1.2801932367149758,
                "no_speech_prob": 0.58056640625,
                "words": null
            }
        ],
        "language": "yue"
    }

    output : dict
    
    {
        "guid": "string",
        "file_name": "string", 
        "call_nature": "string",
        "summary": "string",
        "agent": "string",
        "customer_id": "int",
        "conversation_time": "timestamp",
        "conversation_duration": "double",
        "conversation_location": "string",
        "language_code": "string",
        "related_products": ["string"],
        "related_location": ["string"],
        "action_items_detected_text": "string",
        "issues_detected_text": "string", 
        "outcomes_detected_text": "string",
        "categories_detected_text": "string",
        "custom_entities": ["string"],
        "categories_detected": ["string"],
        "customer_sentiment_score": "double",
        "agent_sentiment_score": "double",
        "customer_total_time_secs": "double",
        "agent_total_time_secs": "double",
        "raw_transcript_text": "string",
        "raw_segments": [{
            "id": "int",
            "seek": "int", 
            "start": "double",
            "end": "double",
            "text": "string",
            "tokens": ["bigint"],
            "temperature": "double",
            "avg_logprob": "double",
            "compression_ratio": "double",
            "no_speech_prob": "double",
            "words": ["string"]
        }],
        "sys_s3_path": "string",
        "sys_process_time": "timestamp"
    }

    """


    output_file_info = {}

    # Extract information from the filename
    customer_id, guid, agent, conversationtime, remark = extract_info_from_filename(transcript_filename)

    output_file_info['guid'] = guid
    output_file_info['file_name'] = os.path.basename(transcript_filename)
    # call_nature
    # summary
    output_file_info['agent'] = agent
    output_file_info['customer_id'] = customer_id
    output_file_info['conversation_time'] = convert_isoformat(conversationtime)
    output_file_info['conversation_duration'] = transcript_data['segments'][-1]['end'] - transcript_data['segments'][0]['start']
    output_file_info['conversation_location'] = 'Hong Kong'
    output_file_info['language_code'] = transcript_data['language']
    # related_products
    # related_location
    # action_items_detected_text
    # issues_detected_text
    # outcomes_detected_text
    # categories_detected_text
    # custom_entities
    # categories_detected
    output_file_info['customer_sentiment_score'] = randint(0, 4) # TODO: Identify sentiment from content. need to identify which is customer
    output_file_info['agent_sentiment_score'] = randint(0, 4) # TODO: Identify sentiment from content. need to identify which is agent
    output_file_info['customer_total_time_secs'] = randint(10, 300)
    output_file_info['agent_total_time_secs'] = randint(0, 4)
    output_file_info['raw_transcript_text'] = transcript_data['text']
    output_file_info['raw_segments'] = transcript_data['segments']
    output_file_info['segments'] = transcript_data['segments'] # TODO: Upload all items

    # invoke bedrock to extract entities
    PROMPT_TEMPLATE, modelId, inference_configuration = get_prompt_template(prompt_identifier, prompt_version)

    print(PROMPT_TEMPLATE)

    body = json.dumps({
        "message": PROMPT_TEMPLATE.replace("{{calllog}}", output_file_info['raw_transcript_text']),
        "max_tokens": inference_configuration['text']['maxTokens'],
        "temperature": inference_configuration['text']['temperature'],
        "p": inference_configuration['text']['topP']
    })

    modelId = modelId
    accept = 'application/json'
    contentType = 'application/json'

    response = brclient.invoke_model(body=body, modelId=modelId, accept=accept, contentType=contentType)

    response_body = json.loads(response.get('body').read())

    # text
    parsed_output = json.loads(response_body.get('text'))
    # {
    #     "related_products" : "手機服務",
    #     "related_location" : "null",
    #     "action_items_detected_text" : ["繳電話費", "開機"],
    #     "issues_detected_text" : ["未能及時開通電話"],
    #     "outcomes_detected_text" : ["客戶繳納電話費", "客服承諾半小時內開通電話"],
    #     "categories_detected_text" : "帳户查詢,技術支援",
    #     "custom_entities" : ["60673031"],
    #     "categories_detected" : ["帳户查詢", "技術支援"],
    #     "call_nature" : "技術支援",
    #     "summary" : "胡先生詢問未能及時開通電話的原因，並告知已繳納電話費。客服代表黃小姐確認了登記資料，承諾半小時內開通電話。"
    # }

    output_file_info["related_products"] = parsed_output["related_products"]
    output_file_info["related_location"] = parsed_output["related_location"]
    output_file_info["action_items_detected_text"] = re.sub(r"[\'\[\]\{\}]", "", str(parsed_output["action_items_detected_text"]))
    output_file_info["issues_detected_text"] = re.sub(r"[\'\[\]\{\}]", "", str(parsed_output["issues_detected_text"]))
    output_file_info["outcomes_detected_text"] = re.sub(r"[\'\[\]\{\}]", "", str(parsed_output["outcomes_detected_text"]))
    output_file_info["categories_detected_text"] = re.sub(r"[\'\[\]\{\}]", "", str(parsed_output["categories_detected_text"]))
    output_file_info["custom_entities"] = parsed_output["custom_entities"]
    output_file_info["categories_detected"] = parsed_output["categories_detected"]
    output_file_info["call_nature"] = parsed_output["call_nature"]
    output_file_info["summary"] = parsed_output["summary"]

    # print(file_info)
    return output_file_info

def convert_isoformat(date_str:str = "2024-10-01T14-02-40"):

    # Convert string to datetime object
    # Replace the hyphens in time part with colons
    temp = date_str.split('T')
    formatted_str = " ".join([temp[0],temp[1].replace('-',':')])

    return formatted_str

def lambda_handler(event, context):
    """Lambda function that read the transcription results, extract a list of entity and sentiments,
    and then throw the parsed results to an iceberg voc-output table through data firehose.

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: Object containing details of the stock buying transaction
    """
    
    try:
        
        transcript_key = event['output_key']
        transcript_filename = os.path.basename(transcript_key)
        
        #  Get output bucket
        output_parameters = _get_ssm_parameters()
        
        # Read the transcription JSON from S3
        response = s3_client.get_object(Bucket=output_parameters['output_bucket'], Key=transcript_key)
        transcript_data = json.loads(response['Body'].read().decode('utf-8'))

        # 
        file_info = extract_entity_using_llm(transcript_filename, transcript_data, output_parameters['extract_entity_prompt_identifier'], output_parameters['extract_entity_prompt_version'], output_parameters['extract_entity_prompt_variant'])

        file_info['sys_s3_path'] = transcript_key
        file_info['sys_process_time'] = convert_isoformat(datetime.now().isoformat())

        print(file_info)

        # Construct the full INSERT query
        query = format_athena_insert_query(file_info, output_parameters['glue_db'], output_parameters['glue_table_processed_transcription'])

        print(query)
        # query = f"""
        #     INSERT INTO voc_db.voc_processed_transcription (guid, file_name, call_nature, summary, agent, customer_id, conversation_time, conversation_duration, conversation_location, language_code, related_products, related_location, action_items_detected_text, issues_detected_text, outcomes_detected_text, categories_detected_text, custom_entities, categories_detected, customer_sentiment_score, agent_sentiment_score, customer_total_time_secs, agent_total_time_secs, raw_transcript_text, raw_segments, sys_s3_path, sys_process_time)
        #     VALUES (
        #         CAST (UUID() as varchar),
        #         'call_recording_1.wav',
        #         '技術支援',
        #         '客戶詢問技術支援問題',
        #         'ChrisK',
        #         1,
        #         TIMESTAMP '2024-10-15 14:30:00',
        #         120,
        #         '中西區',
        #         'yue',
        #         ARRAY['光纖寬頻'],
        #         ARRAY['香港'],
        #         NULL,
        #         NULL,
        #         NULL,
        #         '技術支援',
        #         ARRAY['技術支援', '問題'],
        #         ARRAY['技術支援'],
        #         3,
        #         4,
        #         NULL,
        #         NULL,
        #         '客戶詢問技術支援問題',
        #         NULL,
        #         's3://voc-output-144608043951/TranscribedOutput/call_recording_1.wav',
        #         CURRENT_TIMESTAMP
        #     );
        # """

        # Start query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': output_parameters['glue_db']
            },
            WorkGroup='primary'  # Use your workgroup name
        )

    except Exception as e:
        print(f"Error reading transcript from S3: {e}")
        raise e

def format_segments_to_array(segments):
    formatted_segments = []
    
    for segment in segments:
        row = (
            segment['id'],
            segment['seek'],
            segment['start'],
            segment['end'],
            f"'{segment['text']}'",  # Quote the text with single quotes
            f"ARRAY{segment['tokens']}",
            segment['temperature'],
            segment['avg_logprob'],
            segment['compression_ratio'],
            segment['no_speech_prob'],
            f"ARRAY{segment['words']}" if segment['words'] is not None else "ARRAY[]"
        )
        formatted_segments.append(f"ROW({', '.join(map(str, row))})")
    
    return f"ARRAY[{', '.join(formatted_segments)}]"

def format_athena_insert_query(file_info, database, table):

    def _format_array(array_data):
        if isinstance(array_data, str):
            return f"""ARRAY['{array_data}']"""
        else:
            return f"""ARRAY[{','.join(f"'{x}'" for x in array_data)}]"""

    return f"""
            INSERT INTO {database}.{table} VALUES
                (
                '{file_info['guid']}',
                '{file_info['file_name']}',
                '{file_info['call_nature']}',
                '{file_info['summary']}',
                '{file_info['agent']}',
                {file_info['customer_id']},
                TIMESTAMP '{file_info['conversation_time']}',
                {file_info['conversation_duration']},
                '{file_info['conversation_location']}',
                '{file_info['language_code']}',
                {_format_array(file_info['related_products'])},
                {_format_array(file_info['related_location'])},
                '{file_info['action_items_detected_text']}',
                '{file_info['issues_detected_text']}',  
                '{file_info['outcomes_detected_text']}',
                '{file_info['categories_detected_text']}',
                {_format_array(file_info['custom_entities'])},
                {_format_array(file_info['categories_detected'])},
                {file_info['customer_sentiment_score']},
                {file_info['agent_sentiment_score']},
                {file_info['customer_total_time_secs']},
                {file_info['agent_total_time_secs']},
                '{file_info['raw_transcript_text']}',
                {format_segments_to_array(file_info['segments'])},
                '{file_info['sys_s3_path']}',
                TIMESTAMP '{file_info['sys_process_time']}'
                )
        """




if __name__ == '__main__':
    event = {
        'bucket': "voc-input-144608043951",
        'key': 'test/CUST_00001_GUID_0000_AGENT_MelodyL_DT_2024-10-01T14-02-40_ChinaTelecomWong.wav',
        'output_key': "transcribedOutput/CUST_00001_GUID_0000_AGENT_MelodyL_DT_2024-10-01T14-02-40_ChinaTelecomWong.wav.json"
    }

    lambda_handler(event, None)

