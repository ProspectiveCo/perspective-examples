#!/bin/bash

# change this to download and install newer versions of TDengine
TDENGINE_VERSION="3.3.5.0"


TAR_BALL="TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz"
DIR_NAME="TDengine-client-${TDENGINE_VERSION}"

# cleanup previous installations
# delete the tarball and the directory if they exist
rm -f $TAR_BALL
rm -rf $DIR_NAME

# download the tarball
echo "downloading tdengine client: $TAR_BALL"
wget https://www.tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz
tar -xzf $TAR_BALL
rm -rf $TAR_BALL

# symlink the directory to tdengine-client
echo "creating symlinks..."
ln -sfn $DIR_NAME tdengine-client
cd tdengine-client/driver
ln -sfn libtaos.so.${TDENGINE_VERSION} libtaos.so
cd ../..

# add the driver lib to python LD_LIBRARY_PATH
echo "setting LD_LIBRARY_PATH..."
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver
# add this line to your .bashrc or .bash_profile to make it permanent
echo -e "\n# TDengine client driver" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver" >> ~/.bashrc

# installation successful
echo "installation successful!"
echo "next steps:"
echo "1. source your .bashrc or .bash_profile"
echo "2. start a new tdengine docker container: https://docs.tdengine.com/get-started/deploy-in-docker/"
echo "3. create a new python3 virtualenv and pip install taospy"
echo "4. run the example in tdengine-client/examples/python/query_example.py"
