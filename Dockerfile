# Pull base image.
FROM python

# Install dependencies
RUN pip install psycopg2
RUN pip install redis

#Define working directory
RUN cd /tmp
RUN mkdir -p /src
WORKDIR /src
ADD . /src

#CMD python /src/populate_redis_worker.py 10
ENTRYPOINT ["python","/src/populate_redis_worker.py"]
CMD ["10"]