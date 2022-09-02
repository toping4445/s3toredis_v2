FROM python:3.7-slim
MAINTAINER yelo.blood <yelo.blood@kakaopaycorp.com>
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN chmod +x /app/s3toredis.py

CMD ["python","/app/s3toredis.py"]
