import argparse

from components.datamock.mockc_core import read_sample_csv, write_mock

parser = argparse.ArgumentParser(description='command line arguments')
parser.add_argument('--sample-file', type=str,
                    help='采样demo数据，结果是csv', required=True,
                    default="")
parser.add_argument('--output-dirs', type=str,
                    help='', required=False,
                    default="/tmp")

parser.add_argument('--mock-rows', type=int,
                    help='', required=False,
                    default=100000)
parser.add_argument('--max-rows-per-file', type=int,
                    help='', required=False,
                    default=10000000)

if __name__ == '__main__':
    args = vars(parser.parse_args())
    sample_file: str = args["sample_file"]
    output_dirs: str = args["output_dirs"]
    mock_rows: int = int(args["mock_rows"])
    max_rows_per_file: int = int(args["max_rows_per_file"])

    if not sample_file.endswith(".csv"):
        assert "当前只支持csv作为采样文件，横轴代表列明，纵轴代表采样范围"

    columns = read_sample_csv(sample_file)
    write_mock(output_dirs, columns, mock_rows, max_rows_per_file)
