FROM python:3.6

CMD ["python", "/server.py"]

RUN \
  pip install kafka-python && \
  rm -fr /root/.cache

COPY ./server.py /server.py
