#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Basic usage:

> vim ~/.bashrc
export PYTHONPATH=$PYTHONPATH:$HOME/.local/lib/python2.7/site-packages/ 
export PEGASUS_CONFIG_PATH=$HOME/work/conf_pegasus
export PEGASUS_SHELL_PATH=$HOME/work/pegasus
> pip install --user click
> ./pegasus_check_clusters.py --env c3srv
"""

import os
import click

from py_utils import *


@click.command()
@click.option(
    "--env", default="", help="Env of pegasus cluster, eg. c3srv or c4tst")
@click.option('-v', '--verbose', count=True)
def main(env, verbose):
    pegasus_config_path = os.getenv("PEGASUS_CONFIG_PATH")
    if pegasus_config_path is None:
        echo(
            "Please configure environment variable PEGASUS_CONFIG_PATH in your bashrc or zshrc",
            "red")
        exit(1)
    if env != "":
        echo("env = " + env)
    set_global_verbose(verbose)
    clusters = list_pegasus_clusters(pegasus_config_path, env)
    for cluster in clusters:
        echo("=== " + cluster.name())
        try:
            cluster.print_imbalance_nodes()
            cluster.print_unhealthy_partitions()
        except RuntimeError as e:
            echo(str(e), "red")
            return
        echo("===")


if __name__ == "__main__":
    main()
