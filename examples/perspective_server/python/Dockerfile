FROM python:3.12-bookworm

WORKDIR /app

COPY requirements.txt server.py index.html entrypoint.sh ./

RUN pip install --verbose -r requirements.txt

# expose perspective-server websocket port (8080) and index.html http port (8000)
EXPOSE 8080 8000
ENTRYPOINT ["./entrypoint.sh"]
