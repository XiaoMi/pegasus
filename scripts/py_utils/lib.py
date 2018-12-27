#!/usr/bin/python
#
# Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
# This source code is licensed under the Apache License Version 2.0, which
# can be found in the LICENSE file in the root directory of this source tree.

import click
import commands
import os

_global_verbose = False


def set_global_verbose(val):
    _global_verbose = val


def echo(message, color=None):
    click.echo(click.style(message, fg=color))


class PegasusCluster(object):
    def __init__(self, cfg_file):
        self._cluster_name = os.path.basename(
            cfg_file.name).lstrip("pegasus-").rstrip(".cfg")
        self._shell_path = os.getenv("PEGASUS_SHELL_PATH")
        if self._shell_path is None:
            echo(
                "Please configure environment variable PEGASUS_SHELL_PATH in your bashrc or zshrc",
                "red")
            exit(1)

    def print_unhealthy_partitions(self):
        list_detail = self._run_shell("ls -d").strip()

        read_unhealthy_app_count = int([
            line for line in list_detail.splitlines()
            if line.startswith("read_unhealthy_app_count")
        ][0].split(":")[1])
        write_unhealthy_app_count = int([
            line for line in list_detail.splitlines()
            if line.startswith("write_unhealthy_app_count")
        ][0].split(":")[1])

        if write_unhealthy_app_count > 0:
            echo("cluster is write unhealthy, write_unhealthy_app_count = " +
                 str(write_unhealthy_app_count))
            return
        if read_unhealthy_app_count > 0:
            echo("cluster is read unhealthy, read_unhealthy_app_count = " +
                 str(read_unhealthy_app_count))
            return

    def print_imbalance_nodes(self):
        nodes_detail = self._run_shell("nodes -d").strip()

        primaries_per_node = []
        for line in nodes_detail.splitlines()[1:]:
            columns = line.strip().split()
            if len(columns) < 5 or not columns[4].isdigit():
                continue
            primary_count = int(columns[3])
            primaries_per_node.append(primary_count)
        primaries_per_node.sort()
        if float(primaries_per_node[0]) / float(primaries_per_node[-1]) < 0.8:
            print nodes_detail

    def _run_shell(self, args):
        """
        :param args: arguments passed to ./run.sh shell (type `string`)
        :return: shell output
        """
        global _global_verbose

        cmd = "cd {1}; echo {0} | ./run.sh shell -n {2}".format(
            args, self._shell_path, self._cluster_name)
        if _global_verbose:
            echo("executing command: \"{0}\"".format(cmd))

        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            raise RuntimeError("failed to execute \"{0}\": {1}".format(
                cmd, output))

        result = ""
        result_begin = False
        for line in output.splitlines():
            if line.startswith("The cluster meta list is:"):
                result_begin = True
                continue
            if line.startswith("dsn exit with code"):
                break
            if result_begin:
                result += line + "\n"
        return result

    def name(self):
        return self._cluster_name


def list_pegasus_clusters(config_path, env):
    clusters = []
    for fname in os.listdir(config_path):
        if not os.path.isfile(config_path + "/" + fname):
            continue
        if not fname.startswith("pegasus-" + env):
            continue
        if not fname.endswith(".cfg"):
            continue
        if fname.endswith("proxy.cfg"):
            continue
        f = open(config_path + "/" + fname, "r")
        clusters.append(PegasusCluster(f))
    return clusters
