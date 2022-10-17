FROM python:3.10

RUN apt-get update && apt-get install -y \
    zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /src /out
WORKDIR /src
ADD requirements.txt .
RUN python -m pip install -r requirements.txt

ADD report.py version ./
ADD sql ./sql/

CMD ["./report.py", "--sql", "sql", "--output", "/out/report.html", "--verbose"]
