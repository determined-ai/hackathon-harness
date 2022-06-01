"""
Hijack the launcher from our core_api demo.
"""

import logging
import os
import pathlib
import queue
import subprocess
import sys
import threading
import time

import determined as det


# NEW: Launch one process per slot.  In many distributed training frameworks, like horovod,
# torch.distributed, or deepspeed, there is a launcher of some sort provided by the framework.
# This example implements a launcher from scratch using subprocess and threading.
def launcher_main(subcmd, slots_per_node, num_nodes, cross_rank, chief_ip):
    # Use subprocess to start one worker process per node.
    procs = []
    for local_rank in range(slots_per_node):
        rank = cross_rank * slots_per_node + local_rank
        size = slots_per_node * num_nodes
        env = dict(os.environ)
        env["RANK"] = str(rank);
        env["SIZE"] = str(size);
        env["CHIEF_IP"] = chief_ip;
        cmd = [
            # Use the determined.launch.wrap_rank to wrap the worker process.
            # This ensures logs from each worker can be filtered by rank in the WebUI.
            "python3",
            "-m",
            "determined.launch.wrap_rank",
            str(rank),
            "--",
            *subcmd,
        ]
        procs.append(subprocess.Popen(cmd, env=env))

    # A good launcher normally waits for all workers to finish, but cleans up and exits
    # nonzero immediately if any worker fails to prevent distributed training jobs from
    # hanging.  One way to do this by managing each worker process in a thread and sending
    # exit codes over a Queue as workers complete.
    q = queue.Queue()

    def wait_for_worker(proc):
        worker_exit = proc.wait()
        q.put((proc, worker_exit))

    threads = [threading.Thread(target=wait_for_worker, args=(proc,)) for proc in procs]

    for t in threads:
        t.start()

    first_failed_exit = 0
    for i in range(slots_per_node):
        proc, worker_exit = q.get()
        procs.remove(proc)
        if worker_exit != 0 and first_failed_exit == 0:
            # When the first worker crashes, preempt the others.
            first_failed_exit = worker_exit
            for proc in procs:
                proc.kill()

    for t in threads:
        t.join()

    return first_failed_exit


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=det.LOG_FORMAT)

    info = det.get_cluster_info()
    assert info is not None, "this example only runs on-cluster"
    latest_checkpoint = info.latest_checkpoint
    trial_id = info.trial.trial_id
    hparams = info.trial.hparams

    # NEW: gather rank information from the ClusterInfo API.
    slots_per_node = len(info.slot_ids)
    num_nodes = len(info.container_addrs)
    cross_rank = info.container_rank
    chief_ip = info.container_addrs[0]

    subcmd = sys.argv[1:]
    assert subcmd, "no subcmd provided"

    exitcode = launcher_main(subcmd, slots_per_node, num_nodes, cross_rank, chief_ip)
    sys.exit(exitcode)
