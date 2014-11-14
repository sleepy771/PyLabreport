__author__ = 'filip'

import abc


class Stream(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def read_ln(self):
        pass

    @abc.abstractmethod
    def read_json(self):
        pass


class Reader(object):

    def __init__(self, stream):
        self.stream = stream

    def read_float(self):
        return float(self.stream.read_ln())

    def read_string(self):
        return self.stream.read_ln()

    def read_int(self):
        return int(self.stream.read_ln())

    def read_object(self, obj_clazz):
        pass


class IData(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, column, row_index):
        pass

    @abc.abstractmethod
    def set(self, column, row_index, value):
        pass

    @abc.abstractmethod
    def add_column(self, col_name, values):
        pass

    @abc.abstractmethod
    def set_column(self, col_name, values):
        pass

    @abc.abstractmethod
    def swap_rows(self, swap_table):
        pass

    @abc.abstractmethod
    def swap_columns(self, swap_table):
        pass

    @abc.abstractmethod
    def order_columns(self, cols):
        pass

    @abc.abstractmethod
    def order_columns_by(self, ordering_closure):
        pass

    @abc.abstractmethod
    def remove_column(self, col_name):
        pass

    @abc.abstractmethod
    def row_size(self):
        pass

    @abc.abstractmethod
    def columns_size(self):
        pass

    @abc.abstractmethod
    def list_columns(self):
        pass

    @abc.abstractmethod
    def get_column(self, name):
        pass

    @abc.abstractmethod
    def remove_row(self, idx):
        pass

    @abc.abstractmethod
    def add_row(self, row, fill):
        pass

    @abc.abstractmethod
    def set_row(self, idx, row, fill):
        pass

    @abc.abstractmethod
    def get_row(self, idx, *columns):
        pass

    @abc.abstractmethod
    def order_rows_by(self, ordering_closure):
        pass

    @abc.abstractmethod
    def find_rows(self, finder, *columns):
        pass

    @abc.abstractmethod
    def find_rows_indexes(self, finder):
        pass

    @abc.abstractmethod
    def set_default_null(self, null_fill=None):
        pass

    @abc.abstractmethod
    def set_null(self, **kwargs):
        pass

    def find_row_index(self, finder):
        return self.find_rows_indexes(finder)[0]

    def find_row(self, finder, *columns):
        return self.find_rows(finder, *columns)[0]


# Definition of data table
class Data(IData):
    def __init__(self, name, columns=list(), data_lst=None, default_null=None):
        self._name = name
        if not data_lst:
            data_lst = list((list() for k in columns))
        self._table = Data._create_table(columns, data_lst)
        # to achieve some sort of ordering
        self._columns = columns + list(key for key in self._table.keys() if key not in self._columns)
        keys = self._table.keys()
        self._rows_count = len(self._table[keys[0]]) if keys else 0
        self._cols_count = len(keys)
        self._default_fill = default_null

    @staticmethod
    def _create_table(cols, data, fill=None):
        k = 0
        n = max(len(cols), len(data))
        rows_max = -1
        for col in data:
            rows_max = max(rows_max, len(col))
        dct = dict()
        while n < k:
            dct[Data._get_column_name(n, cols)] = Data._get_data(n, data, fill=fill, size=rows_max)
        return dct

    @staticmethod
    def _get_column_name(idx, columns):
        return columns[idx] if idx < len(columns) else 'col_%d' % idx

    @staticmethod
    def _get_data(idx, data, fill=None, size=0):
        if idx < len(data):
            if hasattr(data[idx], '__iter__'):
                out_lst = list(data[idx])
            else:
                out_lst = [data[idx], ]
            return out_lst + Data._filled_list(size - len(out_lst), fill=fill)
        else:
            return Data._filled_list(size=size, fill=fill)

    @staticmethod
    def _filled_list(size=0, fill=None):
        if size < 0:
            size = 0
        return list((fill for k in range(size)))

    @staticmethod
    def _create_column(column, size=0, fill=None):
        if hasattr(column, '__iter__'):
            col = list(column)
        else:
            col = [column, ]
        return col + Data._filled_list(size - len(col), fill)

    @staticmethod
    def _swap_table_valid(swp_tab, size):
        from_ = set()
        to_ = set()
        for f, t in swp_tab:
            if f in from_ or t in to_ or f > size or t > size:
                return False
            from_.add(f)
            to_.add(t)
        return True

    def get(self, column_name, row_index):
        self._check_in_table(column_name, row_index)
        return self._table[column_name][row_index]

    def set(self, column_name, row_index, value):
        self._check_in_table(column_name, row_index)
        tmp = self._table[column_name][row_index]
        self._table[column_name][row_index] = value
        return tmp

    def set_column(self, col_name, values):
        self._check_columns_in_table(col_name)
        tmp = self._table[col_name]
        if hasattr(values, '__iter__'):
            values = list(values)
        else:
            values = [values, ]
        self._table[col_name] = values
        self._resize_by_max()
        return tmp

    def swap_rows(self, swap_table):  # ((0, 1), (1,2), (2,3), ..., (n, 0))
        if not Data._swap_table_valid(swap_table, self.row_size()):
            raise ValueError('Invalid swap table!')
        cols = self.list_columns()
        for f, t in swap_table:
            swp_dct = dict()
            for col in cols:
                swp_dct[col] = self._table[col][f]
            swp_dct = self._silent_set_row(t, swp_dct)
            self._silent_set_row(f, swp_dct)

    def swap_columns(self, swap_table):
        pass

    def order_columns(self, cols):
        pass

    def order_columns_by(self, ordering_closure):
        pass

    def list_columns(self):
        return self._columns

    def remove_row(self, idx):
        pass

    def add_row(self, row, fill):
        pass

    def set_row(self, idx, row, fill=None):
        self._check_row_index(idx)
        self._check_row_column_names(row)
        return self._silent_set_row(idx, row, fill)

    def _silent_set_row(self, idx, row, fill=None):
        tmp_row = dict()
        if fill is not None:
            for k, v in self._table.items():
                tmp_row[k] = v[idx]
            for k, v in row.items():
                self._table[k][idx] = v
        else:
            for k, v in self._table.items():
                tmp_row[k] = v[idx]
                self._table[k][idx] = row[k] if k in row else fill
        return tmp_row

    def get_row(self, idx, *columns):
        self._check_row_index(idx)
        self._check_columns_in_table(columns)
        return self._silent_set_row(idx, *columns)

    def _silent_get_row(self, idx, *columns):
        if not columns:
            return dict((k, v[idx]) for k, v in self._table.items())
        return dict((col, self._table[col][idx]) for col in columns)

    def order_rows_by(self, ordering_closure):
        pass

    def find_rows(self, finder, *columns):
        self._check_columns_in_table(*columns)
        indexes = self.find_rows_indexes(finder)
        row_lst = list()
        for idx in indexes:
            row = dict()
            for k, v in self._table:
                row[k] = v[idx]
            row_lst.append(row)
        return row_lst

    def find_rows_indexes(self, finder):
        return finder(self._table)

    def add_column(self, col_name, values):
        if col_name not in self._table:
            self._table[col_name] = Data._create_column(values, size=self.row_size())
        else:
            self._fill_column_with_values(col_name, values)

    def _check_row_column_names(self, row):
        for col in row.keys():
            if col not in self._table:
                raise ValueError('Column %s does not exist in table' % col)

    def _fill_column_with_values(self, column_name, values):
        if column_name not in self._table:
            self._table[column_name] = list()
        column = self._table[column_name]
        if hasattr(values, '__iter__'):
            values = list(values)
        else:
            values = [values, ]
        column.extend(values)
        self._resize_by_max()

    def _resize_by_max(self):
        rows = self.row_size()
        for v in self._table.values():
            rows = max(rows, len(v))
        for v in self._table.values():
            v.extend(Data._filled_list(rows-len(v)))

    def _check_in_table(self, column_name, row_index):
        if column_name not in self._table or row_index >= self.row_size():
            raise ValueError('Table does not contain any value in column %s row %d' % column_name, row_index)

    def _check_columns_in_table(self, *column_names):
        for col_name in column_names:
            if col_name not in self._table:
                raise ValueError('Table does not have column %s' % col_name)

    def _check_row_index(self, idx):
        if idx >= self.row_size():
            raise ValueError('Table size exceeded with index %d and table size %d' % idx, self.row_size())

    def remove_column(self, col_name):
        if col_name in self._table:
            del self._table[col_name]

    def row_size(self):
        return self._rows_count

    def columns_size(self):
        return self._cols_count

    def get_column(self, col_name):
        if col_name in self._table:
            return self._table[col_name]
        return None

    def set_default_null(self, null_fill=None):
        pass

    def set_null(self, **kwargs):
        pass