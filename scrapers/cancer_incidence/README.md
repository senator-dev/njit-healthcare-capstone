cd Docker
docker build -t image-name .
docker run -v STORE_PATH:/app/outputs -it image-name
cd app
conda activate dev-env
python get_start.py
python get_values.py