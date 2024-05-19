# Readme

## Dayly report on Top 10 best-selling resources in each category

Follow these steps to execute **top10.py** in DataProc:

1. select your project:
gcloud config set project stripe-big-3

2. enable API for Cloud Storage and Dataproc

3. Upload requirements.txt and top10.py to Cloud Storage:
`gsutil cp top10.py gs://top-10-main/top10.py
gsutil cp requirements.txt gs://top-10-main/requirements.txt`

4. create a cluster:
`gcloud dataproc clusters create dataproc1 --region us-east1 --single-node --image-version=2.0`

5. submit job:
`gcloud dataproc jobs submit pyspark gs://top-10-main/top10.py --region us-east1 --cluster=dataproc1 --files=gs//top-10-main/requirements.txt`



## Royalties
Follow these steps to schedule and execute **royalties.py** with Scheduler:

1. Create a topic:
`gcloud pubsub topics create monthly_report_topic`

2. Deploy Cloud Function
`gcloud functions deploy royalties --runtime python312 --trigger-topic monthly-report-topic`

3. Create Google Scheduler job:
`prev_month=$(date -d "last month" +%m)
gcloud scheduler jobs create pubsub monthly_report_job --schedule "0 0 1 * *" --topic monthly_report_topic --message-body "$prev_month"`

## Monthly reports on platform usage by resource for country and for time zone

Follow these steps to execute **platform_usage.py** in DataProc:

1. select your project:
gcloud config set project stripe-big-3

2. Upload requirements.txt and platform_usage.py to Cloud Storage:
`gsutil cp top10.py gs://usage-platform-big-3/platform_usage.py
gsutil cp requirements.txt gs://usage-platform-big-3/requirements.txt`

3. create a cluster:
``gcloud dataproc clusters create dataproc3 --region us-east1 --single-node --image-version=2.0``

4. submit job:
`gcloud dataproc jobs submit pyspark gs://usage-platform-big-3/platform_usage.py --region us-east1 --cluster=dataproc3 --files=gs//usage-platform-big-3/requirements.txt`


