#!/usr/bin/env bash

set -eou pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd "${SCRIPT_DIR}" > /dev/null || exit 1
pushd "$(git rev-parse --show-toplevel)" > /dev/null || exit 1

. tools/.library.sh

if [[ $# -eq 0 ]]; then
	FILES=$(git_files | xargs)
else
	FILES=$*
fi
exit_code=0

for fn in ${FILES}; do
	if [[ "${fn}" == *".sh" || "${fn}" == *".zsh" || "${fn}" == *".bash" ]]; then
		if [[ ! -x "${fn}" ]]; then
			echo "${fn} is not executable, but probably should be"
			exit_code=1
		fi
	fi
done

popd > /dev/null || exit 1
popd > /dev/null || exit 1
exit "${exit_code}"
