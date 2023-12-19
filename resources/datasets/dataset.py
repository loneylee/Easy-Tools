class DataSetBase(object):
    def __init__(self, use_bucket_: bool = True, use_partition_: bool = False):
        self.use_bucket = use_bucket_
        self.use_partition = use_partition_

    # return {table_name, TableEntry}
    def get_tables(self) -> dict:
        pass

    def set_external_path(self, external_path_: str):
        pass
