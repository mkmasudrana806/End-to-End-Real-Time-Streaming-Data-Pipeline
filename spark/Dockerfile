FROM apache/spark:python3

USER root


RUN mkdir -p /opt/spark/.ivy2/cache /opt/spark/tmp \
    && chown -R spark:spark /opt/spark

ENV HOME=/opt/spark
ENV HADOOP_USER_NAME=spark
ENV SPARK_LOCAL_DIRS=/opt/spark/tmp

USER spark

CMD ["sleep", "infinity"]
