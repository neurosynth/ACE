import logging
logger = logging.getLogger(__name__)


class DataTable:

    ''' Simple class to represent the contents of an HTML table.
        Basically just a grid with array accessor methods and
        some extra validation. '''

    def __init__(self, n_rows, n_cols):
        self.data = [[None] * n_cols for n in range(n_rows)]
        # self.n_rows = n_rows
        self.n_cols = n_cols

    def __getitem__(self, inds):
        if isinstance(inds, int):
            inds = [inds]
        row = self.data[inds[0]]
        return row[inds[1]] if len(inds) > 1 else row

    def __setitem__(self, inds, v):
        self.data[inds[0]][inds[1]] = v

    def to_list(self):
        return self.data

    @property
    def n_rows(self):
        return len(self.data)

    def add_val(self, val, rows=1, cols=1):
        ''' Find next open position and add values to grid '''

        flat = []
        for row in self.data:
            # If row is not a list for some reason, treat as single-item row
            if isinstance(row, list):
                for item in row:
                    flat.append(item)
            else:
                flat.append(row)

        # Only include hashable items in the set (skip unhashable like lists)
        flat_set = set(x for x in flat if not isinstance(x, list))

        if not None in flat_set:
            open_pos = self.n_rows * self.n_cols
            for i in range(rows):
                self.data.append([None] * self.n_cols)

        else:
            # This indexing operation consumes a lot of CPU time for large tables; need to refactor!
            open_pos = flat.index(None)
            ri = open_pos / self.n_cols
            if (ri + rows) > self.n_rows:
                for i in range(round((ri + rows)) - self.n_rows):
                    self.data.append([None] * self.n_cols)

        ri = open_pos // self.n_cols
        ci = open_pos % self.n_cols

        if cols + ci > self.n_cols:
            cols = self.n_cols - ci

        for r in range(rows):
            for c in range(cols):
                if cols > 1:
                    content = '@@%s@%d' % (
                        val, cols) if c == 0 else '@@%s' % val
                else:
                    content = val
                self[ri + r, ci + c] = content
