#!/bin/bash

BASHRC="~/.bashrc"

apt-get -y install g++ gfortran libtool automake autoconf m4 bison flex libcurl4-openssl-dev zlib1g-dev git wget curl libjpeg-dev cmake python cython python-numpy gdb dos2unix antlr libantlr-dev libexpat1-dev libxml2-dev gsl-bin libgsl0-dev udunits-bin libudunits2-0 libudunits2-dev clang zip

# Install HDF5
v=1.8.18
# wget http://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/hdf5-${v}.tar.gz || exit 1
if [[ ! -f hdf5-${v}.tar.gz ]]; then
  wget https://support.hdfgroup.org/ftp/HDF5/current18/src/hdf5-${v}.tar.gz || exit 1
fi
tar -xf hdf5-${v}.tar.gz || exit 1
cd hdf5-${v} || exit 1
prefix="/usr/local/hdf5-$v"
if [ $HDF5_DIR != $prefix ]; then
    echo "Add HDF5_DIR=$prefix to .bashrc"
    echo "" >> $BASHRC
    echo "# HDF5 libraries for python" >> $BASHRC
    echo export HDF5_DIR=$prefix  >> $BASHRC
fi
./configure --enable-shared --enable-hl --prefix=$HDF5_DIR || exit 1
make -j 2 # 2 for number of procs to be used
make install || exit 1
cd ..

# Install Netcdf
v=4.1.3
v=4.4.1.1
if [[ ! -f netcdf-${v}.tar.gz ]]; then
   wget http://www.unidata.ucar.edu/downloads/netcdf/ftp/netcdf-${v}.tar.gz || exit 1
fi
tar -xf netcdf-${v}.tar.gz || exit 1
cd netcdf-${v} || exit 1
prefix="/usr/local/"
if [ $NETCDF4_DIR != $prefix ]; then
    echo "Add NETCDF4_DIR=$prefix to .bashrc"
    echo "" >> $BASHRC
    echo "# NETCDF4 libraries for python" >> $BASHRC
    echo export NETCDF4_DIR=$prefix  >> $BASHRC
fi
CPPFLAGS=-I$HDF5_DIR/include LDFLAGS=-L$HDF5_DIR/lib ./configure --enable-netcdf-4 --enable-shared --enable-dap --prefix=$NETCDF4_DIR
# make check
make  || exit 1
make install || exit 1
