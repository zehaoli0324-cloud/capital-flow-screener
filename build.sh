#!/bin/bash
set -e
python --version
pip install --upgrade pip setuptools wheel
pip install setuptools==65.0.0
pip install -r requirements.txt