worker=$(docker ps --format "{{.Names}}" | grep airflow-worker)
docker cp $worker:opt/airflow/jokes.csv /home/parallelscorelinux1/jokes_airflow/data/jokes.csv