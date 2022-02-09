#!/bin/sh

cd /
cd /home/pi/projects/dashboard_project

source ~/.bashrc
nohup python3 main.py > output.log &
