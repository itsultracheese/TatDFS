FROM python:3.8-alpine

WORKDIR /app

COPY ./ /app

RUN pip3 install -r requirements.txt

EXPOSE 8086

CMD python3 datanode.py