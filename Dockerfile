FROM python:3.10

WORKDIR /code

COPY ./ /code

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

CMD ["python", "-u", "main.py"]

LABEL org.opencontainers.image.source https://github.com/ndrewgele/ohlc-fetcher
