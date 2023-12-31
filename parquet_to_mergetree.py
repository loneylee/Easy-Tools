import argparse

from tools import parquet_to_mergetree

parser = argparse.ArgumentParser(description='command line arguments')
parser.add_argument('--ori-path', type=str,
                    help='', required=False,
                    default="/data/tpch-data-sf10")
parser.add_argument('--output-path', type=str,
                    help='', required=False,
                    default="/data/tpch-data-sf10-bucket")

parser.add_argument('--ori-format', type=str,
                    help='', required=False,
                    default="parquet")


if __name__ == '__main__':
    args = vars(parser.parse_args())
    ori_path: str = args["ori_path"]
    bucket_path: str = args["output_path"]
    ori_format: str = args["ori_format"]

    parquet_to_mergetree.parser(ori_path, bucket_path, ori_format)
