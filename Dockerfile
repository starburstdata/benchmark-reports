FROM python:3.10

RUN mkdir -p /src /out
WORKDIR /src
ADD requirements.txt .
RUN python -m pip install -r requirements.txt

ADD report.py version ./
ADD sql ./sql/

CMD ["./report.py", "--sql", "sql", "--output", "/out/report.html", "--verbose"]
