import functools
import logging
import time
from multiprocessing import Pool
from pathlib import Path
from typing import List, Dict, Optional

import dataclasses
from cluster.cluster import Cluster, Process, kill_process, start_process, StartedProcess

from . import ClusterInfo
from ..utils import get_pyenv_from_env

CLUSTER_FILENAME = "cluster.json"
CURRENT_DIR = Path(__file__).absolute().parent
MONITOR_SCRIPT_PATH = CURRENT_DIR.parent / "monitoring" / "monitor_script.py"
assert MONITOR_SCRIPT_PATH.is_file()


@dataclasses.dataclass
class StartProcessArgs:
    args: List[str]
    host: str
    name: str
    workdir: Optional[Path] = None
    env: Dict[str, str] = dataclasses.field(default_factory=lambda: {})
    init_cmd: List[str] = dataclasses.field(default_factory=lambda: [])


class ClusterHelper:
    def __init__(self, cluster_info: ClusterInfo):
        self.cluster_info = cluster_info
        cluster_info.workdir.mkdir(exist_ok=True, parents=True)

        self.cluster = Cluster(str(cluster_info.workdir))

    @property
    def workdir(self) -> Path:
        return self.cluster_info.workdir

    @property
    def active_nodes(self) -> List[str]:
        return list(self.cluster.nodes.keys())

    @property
    def processes(self) -> List[Process]:
        processes = []
        for node in self.cluster.nodes.values():
            processes += node.processes
        return processes

    def commit(self):
        with open(self.workdir / CLUSTER_FILENAME, "w") as f:
            self.cluster.serialize(f)

    def stop(self, use_sigint=False):
        start = time.time()

        fn = functools.partial(kill_fn, use_sigint)
        self.cluster.kill(fn)
        logging.info(f"Cluster killed in {time.time() - start} seconds")

    def start_processes(self, processes: List[StartProcessArgs]):
        def prepare_workdir(workdir: Path) -> Path:
            workdir = workdir if workdir else self.workdir
            workdir.mkdir(parents=True, exist_ok=True)
            return workdir.absolute()

        pool_args = [
            StartProcessArgs(args=args.args, host=args.host, name=args.name, env=args.env, init_cmd=args.init_cmd,
                             workdir=prepare_workdir(args.workdir)) for args in
            processes
        ]

        logging.info(f"Starting cluster processes: {pool_args}")

        for process in pool_args:
            logging.info(f"Command: {' '.join(process.args)}")
        spawned = []
        if len(pool_args) == 1:
            spawned.append(start_process_pool(pool_args[0]))
        else:
            with Pool() as pool:
                for res in pool.map(start_process_pool, pool_args):
                    spawned.append(res)

        for (process, args) in zip(spawned, pool_args):
            self.cluster.add(process=process, node=args.host, key=args.name)

    def start_monitoring(self, nodes: List[str], observe_processes=False):
        if not self.cluster_info.monitor_nodes:
            return

        init_cmd = []
        pyenv = get_pyenv_from_env()
        if pyenv:
            init_cmd += [f"source {pyenv}/bin/activate"]
        else:
            logging.warning("No Python virtualenv detected. Monitoring will probably not work.")

        nodes = sorted(set(nodes))
        workdir = self.workdir / "monitoring"
        processes = []
        for node in nodes:
            args = ["python", str(MONITOR_SCRIPT_PATH), str(workdir / f"monitoring-{node}.trace")]
            if observe_processes:
                node_processes = self.cluster.get_processes(node=node)
                pids = [str(process.pid) for (_, process) in node_processes]
                args += ["--observe-pids", ",".join(pids)]
            process = StartProcessArgs(
                args=args,
                host=node,
                name=f"monitoring-{node}",
                workdir=workdir,
                init_cmd=init_cmd
            )
            processes.append(process)
        self.start_processes(processes)


def kill_fn(scheduler_sigint: bool, node: str, process: Process):
    signal = "TERM"
    if scheduler_sigint or "monitoring" in process.key:
        signal = "INT"

    if not kill_process(node, process.pgid, signal=signal):
        logging.warning(f"Error when attempting to kill {process} on {node}")


def start_process_pool(args: StartProcessArgs) -> StartedProcess:
    return start_process(args.args, host=args.host, workdir=str(args.workdir), name=args.name, env=args.env,
                         init_cmd=args.init_cmd)