import os

from dagster import get_dagster_logger, job, op


@op
def get_file_sizes():
    files = [f for f in os.listdir(".") if os.path.isfile(f)]
    return {f: os.path.getsize(f) for f in files}


@op
def get_total_size(file_sizes):
    return sum(file_sizes.values())


@op
def get_largest_size(file_sizes):
    return max(file_sizes.values())


@op
def report_file_stats(total_size, largest_size):
    # In real life, we'd send an email or Slack message instead of just logging:
    get_dagster_logger().info(f"****************Total size: {total_size}, largest size: {largest_size}")


@job
def diamond():
    file_sizes = get_file_sizes()
    report_file_stats(
        total_size=get_total_size(file_sizes),
        largest_size=get_largest_size(file_sizes),
    )


# def test_get_total_size():
#     result = get_total_size(get_file_sizes())
#     assert result == 6889

import sys, pytest


@pytest.mark.parametrize
def test_diamond():
    res = diamond.execute_in_process()
    assert res.success
    sys.stdout.write("***********************"+str(dir(res)))
    x = str([x.name for x in res.job_def.all_node_defs])
    for name in x:
        assert res.output_for_node(name) > 0
    # sys.stdout.write("***********************"+str(x))
    # assert res.output_for_node("get_total_size") > 0