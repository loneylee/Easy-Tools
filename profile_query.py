import argparse

from profiles.clickhouse_proflie import gen_ch_profiles

parser = argparse.ArgumentParser(description='command line arguments')
parser.add_argument('--db', type=str,
                    help='', required=False,
                    default="")
parser.add_argument('--query-id', type=str,
                    help='', required=False,
                    default="")

if __name__ == '__main__':
    args = vars(parser.parse_args())
    db: str = args["db"]
    query_id = args["query_id"]

    if db.lower() == 'clickhouse':
        gen_ch_profiles(query_id)
