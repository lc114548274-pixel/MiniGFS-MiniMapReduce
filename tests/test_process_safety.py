import os
import psutil
import pytest
from scripts.process_manager import Manager


def test_pid_identity_requires_matching_command(tmp_path):
    current = psutil.Process(os.getpid())
    record = {'pid': current.pid, 'create_time': current.create_time(), 'args': ['-m', 'not_this_project']}
    assert Manager(tmp_path).verified(record) is None


def test_pid_identity_requires_matching_start_time(tmp_path):
    current = psutil.Process(os.getpid())
    record = {'pid': current.pid, 'create_time': current.create_time()-100, 'args': current.cmdline()[1:]}
    assert Manager(tmp_path).verified(record) is None


@pytest.mark.parametrize('worker', ['master', 'dashboard', '../worker-1', 'worker-99'])
def test_management_rejects_unregistered_or_non_worker(tmp_path, worker):
    with pytest.raises(ValueError):
        Manager(tmp_path).worker_action(worker, 'stop')
