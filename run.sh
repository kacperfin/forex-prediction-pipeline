#!/bin/bash

sudo docker compose up --build --remove-orphans
python3 main.py