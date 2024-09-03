# ingest-tuev-sued
web service to ingest / download data from the jobfeed resource from tuv-sud.com

## invoke on trd-dwh-dev

curl --location --request GET 'https://ingest-tuev-sued-tcvu73msma-ey.a.run.app/?env=staging' \
-H "Authorization: Bearer $(gcloud auth print-identity-token)"