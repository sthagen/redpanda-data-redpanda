#!/usr/bin/env bash

# Make a local ducktape venv. This installs ducktape and rptest packages to ease
# local development in an IDE (which will now be able to find the packages it
# expects) but does _not_ let you run tests locally outside of docker: for that
# you should still use the rp:run-ducktape-tests targets to run them inside docker.

set -euo pipefail

# script expects cwd to be the parent dir of the script
# CC BY-SA 4.0 https://creativecommons.org/licenses/by-sa/4.0/ https://stackoverflow.com/a/246128
cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null

echo "Setting up the venv for local ducktape in ${VENV:=.venv}"

python3 -m venv $VENV
. $VENV/bin/activate

# this should be closely aligned with the install step in ./docker/Dockerfile
python3 -m pip install -e .

echo "Everything installed, now activate the venv in your shell by running:"
echo "source $VENV/bin/activate"
