#!/bin/sh
set -ex

mkdir deps
cd deps
wget https://github.com/jakob-zwiener/Metanome/zipball/master -O metanome.zip
unzip metanome.zip
cd *
mvn -q clean install
