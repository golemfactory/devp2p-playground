#!/bin/sh

../ans-inv/ec2.py |jq -r '._meta.hostvars[] | select(has("ec2_tag_playground")) |"\(.ec2_ip_address)/32"' |xargs -l aws ec2 --region $1 authorize-security-group-ingress --group-id $2 --protocol -1 --cidr
