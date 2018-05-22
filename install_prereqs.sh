#!/bin/sh

# Install packages needed to execute Android tools on 64-bit Linux.
#
# Author: Kevin Litwack

set -e

if [ `uname` = 'Linux' ] && [ `uname -m` = x86_64 ]; then
  echo "Installing Linux 64-bit packages"
  sudo apt-get update -qq
  sudo apt-get install -qq --force-yes libgd2-xpm-dev gcc-multilib
fi

# Workaround to using openjdk7 with Gradle due to security issue:
# https://github.com/gradle/gradle/issues/2421
BCPROV_FILENAME=bcprov-ext-jdk15on-158.jar
wget "https://bouncycastle.org/download/${BCPROV_FILENAME}"
sudo mv $BCPROV_FILENAME /usr/lib/jvm/java-7-openjdk-amd64/jre/lib/ext
sudo perl -pi.bak -e 's/^(security\.provider\.)([0-9]+)/$1.($2+1)/ge' /etc/java-7-openjdk/security/java.security
echo "security.provider.1=org.bouncycastle.jce.provider.BouncyCastleProvider" | sudo tee -a /etc/java-7-openjdk/security/java.security
