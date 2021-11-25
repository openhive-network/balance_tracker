# Installs postgREST v8 to system directory.
# Tested on Ubuntu 20.

#!/bin/bash

set -euo pipefail

wget https://github.com/PostgREST/postgrest/releases/download/v8.0.0/postgrest-v8.0.0-linux-x64-static.tar.xz

POSTGREST=$(find . -name 'postgrest*')
tar xJf $POSTGREST
sudo mv 'postgrest' '/usr/local/bin/postgrest'
rm $POSTGREST
