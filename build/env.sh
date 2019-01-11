#!/bin/sh

set -e

if [ ! -f "build/env.sh" ]; then
    echo "$0 must be run from the root of the repository."
    exit 2
fi

# Create fake Go workspace if it doesn't exist yet.
workspace="$PWD/build/_workspace"
root="$PWD"
ethdir="$workspace/src/github.com/ethereum"
if [ ! -L "$ethdir/go-ethereum" ]; then
    mkdir -p "$ethdir"
    cd "$ethdir"
    ln -s ../../../../../. go-ethereum
    cd "$root"
fi

# Set environment for random beacon
if [ -z "${BLS_ALL_IN_ONE_PATH}" ]; then
  export BLS_ALL_IN_ONE_PATH=$GOPATH/src/github.com/herumi/obsolete-bls-all-in-one
fi
echo "BLS_ALL_IN_ONE_PATH=$BLS_ALL_IN_ONE_PATH"
if [ ! -d "${BLS_ALL_IN_ONE_PATH}" ]; then
  echo "Please provide correct BLS_ALL_IN_ONE_PATH or place it in your GOPATH($GOPATH)."
fi
export CGO_CFLAGS=${CGO_CFLAGS}" -I$BLS_ALL_IN_ONE_PATH/bls/include -DBLS_MAX_OP_UNIT_SIZE=6"
export CGO_LDFLAGS=${CGO_LDFLAGS}" -lbls -lbls_if -lmcl -lgmp -lgmpxx -L$BLS_ALL_IN_ONE_PATH/bls/lib -L$BLS_ALL_IN_ONE_PATH/mcl/lib -lstdc++ -lcrypto"

# Set up the environment to use the workspace.
GOPATH="$workspace"
export GOPATH

# Run the command inside the workspace.
cd "$ethdir/go-ethereum"
PWD="$ethdir/go-ethereum"

# Launch the arguments with the configured environment.
exec "$@"
