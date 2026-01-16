FROM python:3.11

WORKDIR /app
COPY . ./
RUN pip install -r ./proxy.venv
CMD ["python", "main.py"]