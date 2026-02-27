# Activate virtualenv
source /home/hrmneffdii/Desktop/ramadhan-data-pipeline/venv/bin/activate

# Masuk ke project directory
cd /home/hrmneffdii/Desktop/ramadhan-data-pipeline

# Run pipeline
python main.py >> logs/cron.log 2>&1