import subprocess


def test_global():
    # TODO: Call the run-global executable, make sure at minimum a job is
    # submitted successfully
    subprocess.check_output(['echo', 'test'])


def test_children():
    # TODO: Call the run-children executable, make sure at minimum the
    # appropriate child jobs are submitted successfully
    subprocess.check_output(['echo', 'test'])
