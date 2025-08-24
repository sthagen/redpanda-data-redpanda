#!/usr/bin/env bash

# Useful target binary to debug bazel run stuff

# Echo the current working directory
echo "==== debug.sh BEGIN ===="

echo "PWD : $(pwd)"
echo "ARGS (one per line):"
for arg in "$@"; do
  echo "$arg"
done
echo "<end of args>"

# Echo environment variables starting with BAZEL_ or BUILD_
for var in $(env | grep -E '^(BAZEL_|BUILD_)' | cut -d= -f1); do
  echo "$var=${!var}"
done

# Print the name of the parent process
if [[ -f "/proc/$PPID/comm" ]]; then
  echo "Parent process name: $(cat /proc/$PPID/comm)"
else
  echo "Parent process name: unknown (could not read /proc/$PPID/comm)"
fi

# print the process tree leading to this process
if command -v pstree &>/dev/null; then
  echo -n "Process tree: "
  pstree -as $$ || echo "pstree failed"
fi

echo -e "===== debug.sh END ====="
