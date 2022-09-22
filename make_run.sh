#!/usr/bin/zsh
make clean > /dev/null
make -j > /dev/null 2> /dev/null

./rundb