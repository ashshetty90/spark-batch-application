FROM bde2020/spark-python-template:3.1.1-hadoop3.2
LABEL maintainer="Ashish Shetty ashshetty90@gmail.com>"

COPY / .
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt
ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_SUBMIT_ARGS "--driver-memory 4G --py-files /main.py"
ENV SPARK_APPLICATION_PYTHON_LOCATION /main.py
ENV SPARK_APPLICATION_ARGS ""

COPY spark-submit.sh /
RUN python3 -m unittest
# Cron specific changes start
#Install Cron
#RUN apt-get update
#RUN apt-get -y install cron

# Add the cron job
#RUN crontab -l | { cat; echo "* * * * * bash /spark-submit.sh"; } | crontab -
# Cron specific changes end

CMD ["/bin/bash", "/spark-submit.sh"]