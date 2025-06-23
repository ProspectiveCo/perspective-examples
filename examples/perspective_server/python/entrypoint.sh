#!/usr/bin/env sh
# start the perspective websocket server in the background
python server.py &

# then serve index.html on :8000
exec python -m http.server 8000 --bind 0.0.0.0
