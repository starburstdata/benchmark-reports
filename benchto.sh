#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<EOF >&2
Usage: $0 [-h] [-x] -r <VERSIONS> [-t <TRINO_SRC_DIR>]
Tests one or more Trino versions

-h       Display help
-r       Test the specified Trino versions; specify a version number, a commit, or SNAPSHOT to build and test currently checked out commit
-t       Path to the Trino sources directory; where the standard benchmarks are read from
EOF
}

DEBUG=false
TRINO_DIR=$(pwd)

while getopts ":r:t:xh" OPTKEY; do
    case "${OPTKEY}" in
        r)
            IFS=, read -ra VERSIONS <<<"$OPTARG"
            ;;
        t)
            TRINO_DIR=$(realpath "$OPTARG")
            ;;
        x)
            DEBUG=true
            set -x
            ;;
        h)
            usage
            exit 0
            ;;
        '?')
            echo >&2 "ERROR: INVALID OPTION -- ${OPTARG}"
            usage
            exit 1
            ;;

        ':')
            echo >&2 "MISSING ARGUMENT for option -- ${OPTARG}"
            usage
            exit 1
            ;;
        *)
            echo >&2 "ERROR: UNKNOWN OPTION -- ${OPTARG}"
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))
[[ ${1:-} == "--" ]] && shift

if [ "${#VERSIONS[@]}" -eq 0 ]; then
    echo >&2 "ERROR: Option '-r <VERSIONS>' is required."
    usage
    exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR" || exit 1

prefix=benchto-
local_repo=$(cd "$TRINO_DIR" && ./mvnw -B help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
git_repo=https://github.com/trinodb/trino.git

function run() {
    local container_name=$prefix$1
    shift

    local status
    status=$(docker container inspect -f '{{.State.Status}}' "$container_name" 2>/dev/null || echo "unknown")
    if [ "$status" == "running" ]; then
        return 0
    fi
    docker rm --force "$container_name"

    echo "Starting $container_name"
    docker run -d --name "$container_name" "$@"
}

function env_exists() {
    local benchto_url=$1
    local benchto_env=$2
    local resp name
    if ! resp=$(curl -fLOsS \
        -H 'Content-Type: application/json' \
        "http://$benchto_url/v1/environment/$benchto_env") ||
        ! name=$(jq -er '.name' <<<"$resp") ||
        [ -z "$name" ]; then
        return 1
    fi
}

function cleanup() {
    local code=$?
    if [ "$code" -ne 0 ]; then
        echo >&2 "errexit on line $(caller)"
    fi
    if [ "$DEBUG" == false ]; then
        docker rm --force ${prefix}trino
        docker rm --force ${prefix}benchto
        docker rm --force ${prefix}postgres
        [ -z "$RES_DIR" ] || rm -rf "$RES_DIR"
    else
        echo >&2 "Remaining resources:"
        echo >&2 "  Docker containers: ${prefix}trino ${prefix}benchto ${prefix}postgres"
        echo >&2 "  Resource dir: $RES_DIR"
    fi
    exit "$code"
}

trap cleanup ERR

run postgres \
    -e POSTGRES_PASSWORD=pw \
    -e POSTGRES_DB=benchto \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 5432:5432 \
    postgres:latest
sleep 2

benchto_version=0.22
benchto_driver=$local_repo/io/trino/benchto/benchto-driver/$benchto_version/benchto-driver-$benchto_version-exec.jar
# download the driver
(cd "$TRINO_DIR" && ./mvnw -q -C dependency:get -Dtransitive=false -Dartifact=io.trino.benchto:benchto-driver:$benchto_version:jar:exec)
benchto_image=trinodev/benchto-service
benchto_host=localhost
benchto_port=8081
benchto_url=$benchto_host:$benchto_port
run benchto \
    --link ${prefix}postgres \
    -e SPRING_DATASOURCE_URL=jdbc:postgresql://${prefix}postgres:5432/benchto \
    -e SPRING_DATASOURCE_USERNAME=postgres \
    -e SPRING_DATASOURCE_PASSWORD=pw \
    -p$benchto_port:8080 \
    $benchto_image
echo "Waiting for Benctho Service to be ready"
until curl --fail --silent --show-error $benchto_url/ >/dev/null; do sleep 1; done

RES_DIR=$(mktemp -d)

# run HMS with HDFS
cat <<XML >"$RES_DIR/core-site.xml"
<?xml version="1.0"?>
<configuration>

    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>

</configuration>
XML
hms_image=ghcr.io/trinodb/testing/hive3.1-hive:latest
if [ "$(uname -m)" == arm64 ]; then
    hms_image=testing/hive3.1-hive:latest-linux-arm64
fi
run hms \
    -p9083:9083 \
    -v "$RES_DIR"/core-site.xml:/etc/hadoop/conf/core-site.xml \
    "$hms_image"

cat <<INI >"$RES_DIR/config.properties"
#single node install config
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# custom options
query.max-memory-per-node=11GB
memory.heap-headroom-per-node=3GB
INI

cat <<INI >"$RES_DIR/hive.properties"
connector.name=hive
hive.metastore.uri=thrift://${prefix}hms:9083
hive.non-managed-table-writes-enabled=true
hive.storage-format=ORC
hive.allow-drop-table=true
hive.hive-views.enabled=true
INI

for TRINO_VERSION in "${VERSIONS[@]}"; do
    if [ "${#TRINO_VERSION}" -eq 3 ]; then
        echo "🎣 Downloading Docker image for release version ${TRINO_VERSION}"
        trino_image=trinodb/trino:$TRINO_VERSION
        docker pull "$trino_image"
    else
        workdir=$(pwd)
        if [ "${TRINO_VERSION}" != SNAPSHOT ]; then
            workdir=$RES_DIR/trino
            rm -rf "${workdir}"
            time git clone "${git_repo}" "${workdir}" --reference-if-able "${TRINO_DIR}" --dissociate
            git -C "${workdir}" checkout -q "${TRINO_VERSION}"
        fi
        TRINO_SRC_VERSION=$(cd "$workdir" && ./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
        echo "🎯 Building Trino version ${TRINO_SRC_VERSION}"
        TRINO_VERSION=$TRINO_SRC_VERSION
        ignored_packages=(
            :trino-server-rpm
            docs
            :trino-tests
            :trino-faulttolerant-tests
            :trino-plugin-reader
            :trino-product-tests
            :trino-product-tests-launcher
            :trino-server-dev
            :trino-test-jdbc-compatibility-old-driver
            :trino-test-jdbc-compatibility-old-server
        )
        printf -v package_list '%s,' "${ignored_packages[@]/#/!}"
        (
            cd "$workdir" || exit 1
            ./mvnw package --strict-checksums -q -T C1 \
                -DskipTests \
                -Dmaven.site.skip=true -Dmaven.source.skip=true -Dmaven.javadoc.skip=true \
                -Dair.check.skip-all \
                -pl "$package_list"
            ./core/docker/build.sh -a "$(uname -m)"
        )
        trino_image=trino:$TRINO_VERSION-$(uname -m)
    fi

    # environment name must be unique for every run, because it's attributes would get overwritten
    seq=1
    while true; do
        benchto_env=trino-$TRINO_VERSION-$(git log -1 --format='%h')-"$seq"
        env_exists "$benchto_url", "$benchto_env" || break
        ((seq++))
    done

    # NOTICE: the container requires at least 16GB of memory
    # TODO deploy this using helm charts to have a multinode cluster?
    run trino \
        --link ${prefix}hms \
        -p8080:8080 \
        -v "$RES_DIR/config.properties":/etc/trino/config.properties \
        -v "$RES_DIR/hive.properties":/etc/trino/catalog/hive.properties \
        -m16G \
        "$trino_image" \
        /usr/lib/trino/bin/launcher run --etc-dir /etc/trino -Dnode.id=trino -J-XX:MaxRAMPercentage=90
    echo "Waiting for Trino to be ready"
    until docker inspect ${prefix}trino --format "{{json .State.Health.Status }}" | grep -q '"healthy"'; do sleep 1; done

    # create a new Benchto environment
    read -r -d '' template <<'JSON' || true
{
    "version": $version,
    "commit": $commit,
    "startup_logs": $logs
}
JSON
    data=$(jq -n "$template" \
        --arg version "$TRINO_VERSION" \
        --arg commit "$(git log -1 --format='%H')" \
        --arg logs "$(docker logs ${prefix}trino 2>&1)")
    curl \
        -H 'Content-Type: application/json' \
        -d "$data" \
        "http://$benchto_url/v1/environment/$benchto_env"

    read -r -d '' data <<JSON || true
{
    "name": "smoketest",
    "description": "Benchmark executed by the smoketest script"
}
JSON
    curl \
        -H 'Content-Type: application/json' \
        -d "$data" \
        "http://$benchto_url/v1/tag/$benchto_env"

    # make sure this fits the current host by setting factors
    echo "Generating test data"
    # sf10 will take about 5 minutes and will cause the benchto-hms image to grow to about 3GB
    # benchmarking a single environment using sf10 should take up to 30 minutes
    "$TRINO_DIR"/testing/trino-benchto-benchmarks/generate_schemas/generate-tpch.py --factors sf10 --formats orc |
        docker run -i \
            --link ${prefix}trino \
            "$trino_image" \
            java -Dorg.jline.terminal.dumb=true -jar /usr/bin/trino \
            --server ${prefix}trino:8080

    cat <<YAML >"$RES_DIR/application.yaml"
benchmarks: src/main/resources/benchmarks
sql: src/main/resources/sql
query-results-dir: target/results

benchmark-service:
  url: http://${benchto_url}

data-sources:
  presto:
    url: jdbc:trino://localhost:8080
    username: benchto
    driver-class-name: io.trino.jdbc.TrinoDriver

environment:
  name: $benchto_env

presto:
  url: http://localhost:8080
  username: benchto

macros:
  drop-caches:
    command: echo "Dropping caches"
  sleep-4s:
    command: echo "Sleeping for 0.4s" && sleep 0.4

benchmark:
  feature:
    graphite:
      event.reporting.enabled: false
      metrics.collection.enabled: false
    presto:
      metrics.collection.enabled: true
      queryinfo.collection.enabled: true
YAML

    cat <<'YAML' >"$RES_DIR/overrides.yaml"
runs: 5
tpch_300: tpch_sf10_orc
scale_300: 10
tpch_1000: tpch_sf10_orc
scale_1000: 10
tpch_3000: tpch_sf10_orc
scale_3000: 10
prefix: ""
YAML

    # run the benchmark
    echo "Starting the benchmark"
    (
        # application.yaml needs to be in current working directory
        # note there's no --timeLimit, which only makes sense for throughput tests
        cd "$RES_DIR"
        java -Xmx1g \
            -jar "$benchto_driver" \
            --sql "$TRINO_DIR"/testing/trino-benchmark-queries/src/main/resources/sql \
            --benchmarks "$TRINO_DIR"/testing/trino-benchto-benchmarks/src/main/resources/benchmarks \
            --activeBenchmarks=presto/tpch \
            --overrides "$RES_DIR/overrides.yaml" \
            --frequencyCheckEnabled false
    )

    docker rm --force ${prefix}trino
done

cleanup
