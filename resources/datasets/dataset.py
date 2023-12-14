class DataSetBase(object):

    # return {table_name, TableEntry}
    def get_tables(self) -> dict:
        pass

    def set_external_path(self, external_path_: str):
        pass
