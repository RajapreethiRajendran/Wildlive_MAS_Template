FROM python:3.13-slim

WORKDIR /code

RUN adduser --disabled-password --gecos '' --system --uid 1001 python \
  && chown -R python /code

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

USER 1001

CMD ["python", "main.py"]
