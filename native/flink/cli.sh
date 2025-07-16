#!/bin/bash

export version="1.20.1"
pwd_path=$(pwd)
https://dlcdn.apache.org/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz
export SEATUNNEL_HOME="$pwd_path/apache-seatunnel-${version}"

install() {
    echo "Installing Flink version $version..."
    wget "https://archive.apache.org/dist/flink/flink-${version}/flink-${version}-bin-scala_2.12.tgz"

    # it will extract to the $SEATUNNEL_HOME directory
    tar -xzvf "flink-${version}-bin-scala_2.12.tgz"
    # rm "apache-seatunnel-${version}-bin.tar.gz"
    echo "Flink version $version installed successfully."
}

# get command by arguments
command="$1"

# check command
if [ "$command" = "install" ]; then
    install
    elif [ "$command" = "start" ]; then
    start
    elif [ "$command" = "stop" ]; then
    stop
    elif [ "$command" = "run" ]; then
    run "$2"
else
    echo "Invalid command: $command"
    echo "Usage: $0 {install|start|stop|run <config_file>}"
    echo "Example: $0 run ./jobs/v2.batch.config.template"
    exit 1
fi