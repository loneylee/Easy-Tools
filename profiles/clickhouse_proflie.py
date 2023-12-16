import csv
import datetime
import sys

from graphviz import Digraph

from commons.client.clickhouse import CHClient
from commons.utils.readable import to_memory_readable
from profiles.entry import ProfileData
from config import ClickhouseConfig, output_dir


# apt install -y graphviz
# pip3 install diagrams


def read_from_csv(file_full_name: str) -> list:
    datas: list[ProfileData] = []
    with open(file_full_name, "r", newline='') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            pids: list = row[11].replace("[", "").replace("]", "").split(",")
            pids.sort()

            etime = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S.%f')

            datas.append(
                ProfileData(name=row[0],
                            event_time_microseconds=etime,
                            id=row[2],
                            plan_step=row[3],
                            elapsed_us=row[4],
                            input_wait_elapsed_us=row[5],
                            output_wait_elapsed_us=row[6],
                            input_rows=row[7],
                            input_bytes=row[8],
                            output_rows=row[9],
                            output_bytes=row[10],
                            parent_ids=pids,
                            pname=row[0],
                            parent_objs=[],
                            child_objs=[],
                            origin_ids=[],
                            duration=0
                            )
            )

    for data in datas:
        data.to_step_keys(datas)
    return datas


def merge_plan(datas: list):
    merge_map = {}
    for node in datas:
        merge_map[node.get_step_keys()] = []

    for node in datas:
        merge_map[node.get_step_keys()].append(node)

    for step_key in merge_map.keys():
        fnode = merge_map.get(step_key)[0]
        length = len(merge_map.get(step_key))

        origin_ids = []
        total_elapsed_us: int = 0
        min_elapsed_us: int = sys.maxsize
        max_elapsed_us: int = 0

        start_time = fnode.event_time_microseconds
        end_time = fnode.event_time_microseconds + datetime.timedelta(microseconds=int(fnode.elapsed_us))

        total_input_rows = 0
        total_input_bytes = 0
        total_output_rows = 0
        total_output_bytes = 0

        min_input_wait_us: int = sys.maxsize
        max_input_wait_us: int = 0
        min_output_wait_us: int = sys.maxsize
        max_output_wait_us: int = 0
        total_input_wait = 0
        total_output_wait = 0

        for node in merge_map.get(step_key):
            elapsed_us: int = int(node.elapsed_us)
            input_wait: int = int(node.input_wait_elapsed_us)
            output_wait: int = int(node.output_wait_elapsed_us)

            total_elapsed_us = total_elapsed_us + elapsed_us
            min_elapsed_us = min(min_elapsed_us, elapsed_us)
            max_elapsed_us = max(max_elapsed_us, elapsed_us)
            start_time = min(node.event_time_microseconds, start_time)
            end_time = max(node.event_time_microseconds + datetime.timedelta(microseconds=int(node.elapsed_us)),
                           end_time)

            total_input_rows = total_input_rows + int(node.input_rows)
            total_input_bytes = total_input_bytes + int(node.input_bytes)
            total_output_rows = total_output_rows + int(node.output_rows)
            total_output_bytes = total_output_bytes + int(node.output_bytes)

            min_input_wait_us = min(min_input_wait_us, input_wait)
            max_input_wait_us = max(max_input_wait_us, input_wait)
            min_output_wait_us = min(min_output_wait_us, output_wait)
            max_output_wait_us = max(max_output_wait_us, output_wait)

            total_input_wait = total_input_wait + input_wait
            total_output_wait = total_output_wait + output_wait

            origin_ids.append(node.id)
            datas.remove(node)

        duration = int((datetime.datetime.timestamp(end_time) - datetime.datetime.timestamp(start_time)) * 1000)

        new_nodes = ProfileData(name=fnode.name,
                                event_time_microseconds=fnode.event_time_microseconds,
                                id=fnode.id,
                                plan_step=fnode.plan_step,
                                elapsed_us="{}({}, {}, {})".format(
                                    format(int(total_elapsed_us / 1000), ','), format(int(min_elapsed_us / 1000), ','),
                                    format(int(max_elapsed_us / 1000), ','),
                                    format(int(total_elapsed_us / 1000 / length), ',')
                                ),
                                input_wait_elapsed_us="{}({}, {}, {})".format(
                                    format(int(total_input_wait / 1000), ','),
                                    format(int(min_input_wait_us / 1000), ','),
                                    format(int(max_input_wait_us / 1000), ','),
                                    format(int(total_input_wait / 1000 / length), ',')
                                ),
                                output_wait_elapsed_us="{}({}, {}, {})".format(
                                    format(int(total_output_wait / 1000), ','),
                                    format(int(min_output_wait_us / 1000), ','),
                                    format(int(max_output_wait_us / 1000), ','),
                                    format(int(total_output_wait / 1000 / length), ',')
                                ),
                                input_bytes="{}({})".format(format(total_input_bytes, ","),
                                                            to_memory_readable(total_input_bytes)),
                                input_rows=format(total_input_rows, ","),
                                output_rows=format(total_output_rows, ","),
                                output_bytes="{}({})".format(format(total_output_bytes, ","),
                                                             to_memory_readable(total_output_bytes)),
                                parent_ids=fnode.parent_ids,
                                parent_objs=fnode.parent_objs,
                                child_objs=fnode.child_objs,
                                pname=fnode.name + "(" + str(len(merge_map.get(step_key))) + ")",
                                origin_ids=origin_ids,
                                duration=format(duration, ',')
                                )
        datas.append(new_nodes)


def gen_root_plan(datas: list) -> ProfileData:
    merge_plan(datas)

    for data in datas:
        for pid in data.parent_ids:
            if pid != "":
                for d in datas:
                    if d.origin_ids.__contains__(pid):
                        if not data.parent_objs.__contains__(d):
                            data.parent_objs.append(d)

                        if not d.child_objs.__contains__(data):
                            d.child_objs.append(data)

                        break

    for data in datas:
        if len(data.parent_objs) == 0:
            return data


def print_cur_stage_plan(g: Digraph, c, nodes: list, next_stage_child_nodes: list):
    if len(nodes) == 0:
        return

    curr_stage_child_nodes = []
    step = nodes[0].plan_step
    for pnode in nodes:
        pnode: ProfileData = pnode

        for cnode in pnode.child_objs:
            cnode: ProfileData = cnode
            if cnode.plan_step == step:
                c.edges([(cnode.to_node(), pnode.to_node())])
                curr_stage_child_nodes.append(cnode)
            else:
                g.edge(cnode.to_node(), pnode.to_node())
                next_stage_child_nodes.append(cnode)

    print_cur_stage_plan(g, c, curr_stage_child_nodes, next_stage_child_nodes)


def print_next_stage_plan(g: Digraph, nodes: list):
    if len(nodes) == 0:
        return

    step = nodes[0].plan_step
    next_stage_child_nodes = []

    with g.subgraph(name=step) as c:
        c.attr(label=step)
        c.attr(style='filled', color='yellow')
        print_cur_stage_plan(g, c, nodes, next_stage_child_nodes)

    print_plan(g, next_stage_child_nodes)


def print_plan(g: Digraph, nodes: list):
    if len(nodes) == 0:
        return

    step_map: dict = {}

    for pnode in nodes:
        step_map[pnode.plan_step] = []

    for pnode in nodes:
        step_map[pnode.plan_step].append(pnode)

    for step in step_map.keys():
        print_next_stage_plan(g, step_map[step])


def clean_datas(datas: list) -> list:
    for data in datas:
        data.name = data.name.replace(":", "_")

    return datas


def read_from_clickhouse_profile_table(query_id: str) -> list:
    client = CHClient(ClickhouseConfig)

    result: list = client.execute_and_fetchall("""
                        select 
                        name, 
                        event_time_microseconds, 
                        id, 
                        plan_step, 
                        elapsed_us, 
                        input_wait_elapsed_us, 
                        output_wait_elapsed_us, 
                        input_rows, 
                        input_bytes, 
                        output_rows, 
                        output_bytes, 
                        parent_ids 
                    from system.processors_profile_log 
                    where query_id='{}'
                    order by event_time_microseconds asc format CSV
                    """.format(query_id))

    datas = []
    for row in result:
        pids: list = str(row[11]).replace("[", "").replace("]", "").replace(" ", "").split(",")
        pids.sort()

        datas.append(
            ProfileData(name=str(row[0]),
                        event_time_microseconds=row[1],
                        id=str(row[2]),
                        plan_step=str(row[3]),
                        elapsed_us=str(row[4]),
                        input_wait_elapsed_us=str(row[5]),
                        output_wait_elapsed_us=str(row[6]),
                        input_rows=str(row[7]),
                        input_bytes=str(row[8]),
                        output_rows=str(row[9]),
                        output_bytes=str(row[10]),
                        parent_ids=pids,
                        pname=str(row[0]),
                        parent_objs=[],
                        child_objs=[],
                        origin_ids=[],
                        duration=0
                        )
        )

    for data in datas:
        data.to_step_keys(datas)

    return datas


def gen_ch_profiles(query_id: str):
    datas: list = []
    if query_id.endswith(".csv"):
        datas = read_from_csv(query_id)
    else:
        datas = read_from_clickhouse_profile_table(query_id)

    datas = clean_datas(datas)

    root_plan: ProfileData = gen_root_plan(datas)

    # g = Digraph('G', filename=file_name + '-profile.gv', )

    g = Digraph(output_dir + "/" + query_id + '-profile', 'comment', None, None, None, None, "UTF-8",
                {'rankdir': 'TB'},
                {'color': 'black', 'fontcolor': 'black', 'fontname': 'FangSong', 'fontsize': '12', 'style': 'rounded',
                 'shape': 'box'},
                {'color': '#999999', 'fontcolor': '#888888', 'fontsize': '10', 'fontname': 'FangSong'}, None, False)

    for data in datas:
        g.node(data.to_node())

    print_plan(g, [root_plan])
    g.render()
