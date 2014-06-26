#!/bin/sh

# Install packages needed to execute Android tools on 64-bit Linux.
#
# Author: Kevin Litwack

set -e

if [ `uname` = 'Linux' ] && [ `uname -m` = x86_64 ]; then
  echo "Installing Linux 64-bit packages"
  sudo apt-get update -qq
  sudo apt-get install -qq --force-yes libgd2-xpm ia32-libs ia32-libs-multiarch
fi
