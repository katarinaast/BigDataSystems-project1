FROM bde2020/spark-python-template:3.1.2-hadoop3.2

COPY app.py /app/
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

ENV SPARK_APPLICATION_ARGS ""

CMD ["/bin/bash", "/template.sh"]
