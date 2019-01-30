#!/bin/bash

target=$1
user=$2
filepath=$3
destpath=$4
destname=$5

if [[ -z "${destpath}" ]]; then
  destpath="."
fi

if [[ -z "${destname}" ]]; then
  destname=$(basename "${filepath}")
fi

echo "$(date -u +%FT%TZ) $$ sftp ${filepath} to ${user}@${target} starting"
sftp -b - ${user}@${target} <<-EOF
	-mkdir ${destpath}/.tmp
	chdir ${destpath}/.tmp
	put ${filepath} ${destname}
	rename ${destname} ../${destname}
	bye
	EOF
if [[ ${?} != 0 ]]; then
  echo "$(date -u +%FT%TZ) $$ sftp ${filepath} to ${user}@${target} failed"
  exit 1
fi
echo "$(date -u +%FT%TZ) $$ sftp ${filepath} to ${user}@${target} finished"
