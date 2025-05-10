#!/bin/bash

/opt/spark/bin/spark-submit target/scala-2.12/assemblyApp.jar preprocess && \
/opt/spark/bin/spark-submit src/main/python/clusterize.py --numPartitions 10 && \
/opt/spark/bin/spark-submit target/scala-2.12/assemblyApp.jar write