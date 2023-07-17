#!/bin/bash

rm -rf package
mkdir package

pip install -r requirements.txt --target ./package

cd package
zip -r ../weather.zip .

cd ..
zip weather.zip main.py db.py
