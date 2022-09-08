FROM python:3.8-slim
MAINTAINER yelo.blood <yelo.blood@kakaopaycorp.com>

COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
RUN chmod +x /app/s3toredis.py

CMD ["python3","/app/s3toredis.py"]
