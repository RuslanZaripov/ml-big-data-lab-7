# ml-big-data-lab-7

## Description

Stack: Scala, Cassandra, PySpark

## PySpark configuration

- execute the following command to know which host port has been mapped to the container's port 8888

```bash
docker port pyspark-notebook 8888
```

- fetch the notebook token. Your output should resemble this URL: `http://127.0.0.1:8888/lab?token=YOUR_TOKEN_HERE`

```bash
docker logs --tail 3 pyspark-notebook
```

### Steps

```bash
docker exec spark python src/create_database.py \
    --csv-path ./sparkdata/en.openfoodfacts.org.products.csv \
    --delimiter '\t' \
    --table-name openfoodfacts
docker exec clickhouse /scripts/seed_db.sh
docker exec spark python src/clusterize.py --numPartitions 30
```
