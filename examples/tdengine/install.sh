#!/bin/bash

#  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
#  ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
#  ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
#  ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
#  ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
#  ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
#  ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
#  ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
#  ┃ This file is part of the Perspective library, distributed under the terms ┃
#  ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
#  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

# This script downloads and installs the TDengine client driver
# needed for taospy (tdengine python client) to work.
# It downloads the client and sets the python LD_LIBRARY_PATH
# and adds the path to your .bashrc or .bash_profile.


# change this to download and install newer versions of TDengine
TDENGINE_VERSION="3.3.5.0"
TAR_BALL="TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz"
DIR_NAME="TDengine-client-${TDENGINE_VERSION}"


echo "INSTALLING TDENGINE CLIENT DRIVER"
echo "  version=$TDENGINE_VERSION"

# ======================= CLEANUP =================================

# cleanup previous installations
# delete the tarball and the directory if they exist
echo "cleaning up previous installations..."
if [ -f $TAR_BALL ]; then
    echo "removing existing tdengine-client tarball: $TAR_BALL"
    rm -f $TAR_BALL
fi
if [ -L "tdengine-client" ]; then
    echo "removing existing tdengine-client symlink"
    rm tdengine-client
fi
if [ -d $DIR_NAME ]; then
    echo "removing existing tdengine client driver directory: $DIR_NAME"
    rm -rf $DIR_NAME
fi
# try to deactivate any virtualenv if it's active, don't worry if it fails
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi
if [ -d "venv" ]; then
    # remove the existing virtualenv
    rm -rf venv
fi


# ======================= DOWNLOAD AND INSTALL TARBALL =================================

# download the tarball
echo "downloading tdengine client: $TAR_BALL"
wget https://www.tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz
tar -xzf $TAR_BALL
rm -rf $TAR_BALL

# symlink the directory to tdengine-client
echo "setting up tdengine-client dir..."
# instead of symlinking the directory, move the directory to the tdengine-client
# ln -sfn $DIR_NAME tdengine-client
mv $DIR_NAME tdengine-client
cd tdengine-client/driver
ln -sfn libtaos.so.${TDENGINE_VERSION} libtaos.so
cd ../..


# ====================== SET PYTHON LD_LIBRARY_PATH ==========================

# add the driver lib to python LD_LIBRARY_PATH
echo "setting LD_LIBRARY_PATH for taospy tdengine python driver to find its libs..."
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver

# add this line to your .bashrc or .bash_profile to make it permanent
if [ -f ~/.bashrc ]; then
    # grep the existing .bashrc file to see if there's a line that sets the LD_LIBRARY_PATH
    # if it doesn't exist, then add the line to the end of the file
    if ! grep -q "export LD_LIBRARY_PATH=.*tdengine-client" ~/.bashrc; then
        echo -e "\n# TDengine client driver" >> ~/.bashrc
        echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver" >> ~/.bashrc
        echo "tdengine client driver path added to .bashrc"
    else
        echo "WARNING: tdengine client driver path already exists in .bashrc. Skipped setting it again..."
        echo "  please check and make sure it's correct"
    fi
elif [ "$(uname)" == "Darwin" ]; then
    # grep the existing .bash_profile file to see if there's a line that sets the LD_LIBRARY_PATH
    # if it doesn't exist, then add the line to the end of the file
    if ! grep -q "export LD_LIBRARY_PATH=.*tdengine-client" ~/.bash_profile; then
        echo -e "\n# TDengine client driver" >> ~/.bash_profile
        echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver" >> ~/.bash_profile
        echo "tdengine client driver path added to .bash_profile"
    else
        echo "WARNING: tdengine client driver path already exists in .bash_profile. Skipped setting it again..."
        echo "  please check and make sure it's correct"
    fi
else
    echo "WARNING: Unknown OS, please add the following line to your shell profile:"
    echo "  export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$(pwd)/tdengine-client/driver"
fi


# ======================= INSTALL TAOSPY AND OTHER PACKAGES ==========================

# check to see if there's a local python virtualenv dir called venv
#  - if there is, then activate it and install/upgrade taospy and other packages in requirements.txt
# - if there isn't, then create a new virtualenv and install/upgrade taospy and other packages in requirements.txt
echo "checking for python virtualenv..."
if [ -d "venv" ]; then
    echo "activating virtualenv..."
    source venv/bin/activate
    echo "installing taospy and other packages..."
    pip install --upgrade pip
    pip install --upgrade -r requirements.txt
else
    echo "creating new virtualenv (venv) and activating it..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install --upgrade -r requirements.txt
fi


# ======================= TEST THE INSTALLATION ==========================
# test the installation by trying to import the taospy module
echo "testing the installation..."
python -c "import taos"
if [ $? -ne 0 ]; then
    echo "ERROR: taospy import failed!"
    echo "please check the installation and try again."
    exit 1
else
    echo "taos tdengine python driver import successful!"
fi



# installation successful
echo -e "\ninstallation successful!\n"
echo "next steps:"
echo "  1. Source the virtualenv:"
echo "     source venv/bin/activate"
echo "  2. Start a tdengine docker container:"
echo "     ./docker.sh"
echo "  3. To test the database and your driver, run the tdengine example:"
echo "     python tdengine-client/examples/python/connect_example.py"
echo -e "\n\nAdditional Documentation:"
echo "  https://docs.tdengine.com/get-started/deploy-in-docker/"
