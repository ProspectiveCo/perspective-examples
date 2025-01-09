## TDEngine <-> Perspective 

This document explains how to configure Perspective with a TDEngine data source. 

### Getting Started

#### 1. Install TDEngine Client Libraries

- Run the `install.sh` script to download and install the TDEngine client libraries locally. This is necessary for the TDEngine Python SDK (taospy) to function.
- For more information on installing TDEngine's client libs, please refer to [install client library](https://docs.tdengine.com/tdengine-reference/client-libraries/#install-client-driver) docs.

```sh
./install.sh
```

- After the install script runs, please verify if the everything is setup correctly.
- You should see a symlink for `libtaos.so` in:

```sh
ls -l tdengine-client/driver/

total 68488
lrwxrwxrwx 1 warthog warthog       18 Jan  7 16:08 libtaos.so -> libtaos.so.3.3.5.0
-rwxr-xr-x 1 warthog warthog 59186032 Dec 31 03:42 libtaos.so.3.3.5.0
-rwxr-xr-x 1 warthog warthog 10937480 Dec 31 03:42 libtaosws.so
-rw-r--r-- 1 warthog warthog        8 Dec 31 03:42 vercomp.txt
```

- Check if the client lib folder is correctly added to `$LD_LIBRARY_PATH`:

```sh
echo $LD_LIBRARY_PATH
```


#### 2. Pull and Run TDEngine Docker Image

- Pull the TDEngine Docker image.
- Run the `docker.sh` script to start a TDEngine container. This script will also wait for the database to initialize and populate it with data from the TDEngine benchmark.

```sh
docker pull tdengine/tdengine:latest

./docker.sh
```

For complete information on running TDEngine docker engine, please refer to [Get Started with TDengine Using Docker](https://docs.tdengine.com/get-started/deploy-in-docker/) docs.

#### 3. Set Up Python Virtual Environment and Install Dependencies

- Go to the project root dir and install the dependencies (including tdengine's python SDK: taospy) in `requirements.txt`:

```sh
cd perspective-examples
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Alternatively, you can just pip install `taospy`:

```sh
pip install taospy
```

#### 4. Run Perspective Server

- Run the `perspective_server.py` script to start a Perspective server. This server will pull data from TDEngine and stream it into a Tornado WebSocket.

```sh
python perspective_server.py
```
<br/><br/>

### Helpful Resources

- **TDEngine client library examples including python and node.js:** Download [TDEngine's client library](https://docs.tdengine.com/tdengine-reference/client-libraries/#install-client-driver) tar file and unpack it. Look inside the examples directory for a comprehensive list of examples.

- [TDEngine Docker Container with Data](https://docs.tdengine.com/get-started/deploy-in-docker/)
- [TDEngine SQL Reference](https://docs.tdengine.com/basic-features/data-querying/)
- [Inserting data into TDEngine](https://docs.tdengine.com/basic-features/data-ingestion/)

Next steps:
- [Streaming data from TDEngine](https://docs.tdengine.com/advanced-features/stream-processing/)
