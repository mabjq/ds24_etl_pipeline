#!/bin/bash
export PYTHONPATH=/home/mabjq/ml_projects/gold_silver_etl:$PYTHONPATH
cd /home/mabjq/ml_projects/gold_silver_etl
source ~/ml_env/bin/activate
python app/main.py >> logs/cron.log 2>&1