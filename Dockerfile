FROM alpine:3.18

WORKDIR /app

COPY yyj_scraper.py requirements.txt ./

COPY config/crontab /etc/crontabs/root

RUN apk add --no-cache python3 py3-pip && \
    pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

RUN chmod +x yyj_scraper.py

CMD [ "cron" ]