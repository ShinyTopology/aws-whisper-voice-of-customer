-- DROP TABLE voc_db.voc_processed_transcription;
CREATE TABLE voc_db.voc_processed_transcription (
  guid string,
  file_name string,
  call_nature string, 
  summary string,
  agent string,
  customer_id int,
  conversation_time timestamp,
  conversation_duration double,
  conversation_location string,
  language_code string,
  related_products ARRAY<string>,
  related_location ARRAY<string>,
  action_items_detected_text string,
  issues_detected_text string,
  outcomes_detected_text string,
  categories_detected_text string,
  custom_entities ARRAY<string>,
  categories_detected ARRAY<string>,
  customer_sentiment_score double,
  agent_sentiment_score double,
  customer_total_time_secs double,
  agent_total_time_secs double,
  raw_transcript_text string,
  raw_segments ARRAY<STRUCT<id: int,seek: int,start: double,end: double,text: string,tokens: ARRAY<bigint>,temperature: double,avg_logprob: double,compression_ratio: double,no_speech_prob: double,words: ARRAY<string>>>,
  sys_s3_path string,
  sys_process_time timestamp
)
  LOCATION 's3://voc-output-144608043951/ProcessedTranscription/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'SNAPPY'
);

-- DROP TABLE voc_db.voc_single_customer_view;
CREATE TABLE voc_db.voc_single_customer_view (
  customer_id string,
  customer_name string, 
  gender string,
  subscribed_plan string,
  end_of_subcription timestamp,
  customer_tags ARRAY<string>,
  recommended_product string
)
  LOCATION 's3://voc-output-144608043951/SingleCustomerView/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'SNAPPY'
);

-- DROP TABLE voc_db.voc_product;
CREATE TABLE voc_db.voc_product (
  product_name string,
  product_id string,
  product_benefits ARRAY<string>,
  product_description string,
  product_price string,
  objection_handling ARRAY<string>
)
  LOCATION 's3://voc-output-144608043951/Product/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'SNAPPY'
);

