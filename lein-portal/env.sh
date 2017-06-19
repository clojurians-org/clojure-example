my=$(cd -P -- "$(dirname -- "${BASH_SOURCE-$0}")" >/dev/null && pwd -P)
export LEIN_HOME=${my}/lein
alias lein=${LEIN_HOME}/lein.sh
