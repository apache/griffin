#!/bin/bash


if [ $# -eq 1 ]; then
username=$1
echo "$username  ALL=(ALL:ALL) ALL" >> /etc/sudoers
fi
