#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Downloads the UCI "Condition monitoring of hydraulic systems" data set into
# DIR, checks it is exactly the file these numbers were measured on, and
# unpacks it there.
#
#   bench/hydraulic/get.sh DIR
set -eu
dir=${1:?usage: get.sh DIR}
url='https://archive.ics.uci.edu/static/public/447/condition+monitoring+of+hydraulic+systems.zip'
sha256=24128aad2ee45eea7e6b63ebbd9992cdf25d0483a2cebefbfc13bc69079af1f2

mkdir -p "$dir"
zip=$dir/hydraulic.zip
if [ ! -f "$zip" ]; then
    curl -fL --retry 3 -o "$zip.part" "$url"
    mv "$zip.part" "$zip"
fi
echo "$sha256  $zip" | sha256sum -c - || {
    echo "get.sh: $zip is not the expected file; delete it and run again" >&2
    exit 1
}
# Plain file names only: nothing absolute, no directories, no "..".
if unzip -Z1 "$zip" | grep -qvE '^[A-Za-z0-9_][A-Za-z0-9_.-]*$' || unzip -Z1 "$zip" | grep -q '\.\.'; then
    echo "get.sh: the archive names a path outside $dir; refusing to unpack it" >&2
    exit 1
fi
unzip -oq "$zip" -d "$dir"
echo "unpacked $(unzip -Z1 "$zip" | wc -l) files into $dir"
