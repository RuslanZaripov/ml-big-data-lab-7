# ml-big-data-lab-7

## Цель работы

- Получить навыки разработки витрины данных и последующей её интеграции

## Stack

- Scala, Cassandra, PySpark

### Steps

```bash
docker exec -it clickhouse /scripts/seed_db.sh
docker compose up --build
```

## Project Structure

```text
.
├── Dockerfile
├── README.md
├── build.sbt
├── conf
│   └── spark.ini
├── docker-compose.yml
├── entrypoint.sh
├── notebooks
│   ├── openfoodfacts_clustering.ipynb
│   ├── openfoodfacts_preprocessing.ipynb
│   └── word_count.ipynb
├── project
│   ├── build.properties
│   └── plugins.sbt
├── requirements.txt
├── scripts
│   └── seed_db.sh
├── sparkdata
│   ├── en.openfoodfacts.org.products.csv
│   └── googleplaystore_user_reviews.csv
├── src
│   └── main
│       ├── python
│       │   ├── __pycache__
│       │   ├── clusterize.py
│       │   └── logger.py
│       └── scala
│           └── Datamart.scala
└── static
    ├── Лабораторная работа 5 (весна 2025).pdf
    ├── Лабораторная работа 6 (весна 2025).pdf
    └── Лабораторная работа 7 (весна 2025).pdf
```
