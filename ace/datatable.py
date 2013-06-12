import logging
logger = logging.getLogger('ace')
logging.basicConfig()

class DataTable:

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

    def n_rows(self):
        return len(self.data)

    # def n_cols(self):
    #   return len(self.data[0] or 0)
    # Find next open position and add values to grid
    def add_val(self, val, rows=1, cols=1):

        # val = str(val)

        # Flatten list and find next open position
        flat = [item for l in self.data for item in l]

        if not None in flat:
            open_pos = self.n_rows() * self.n_cols
            for i in range(rows):
                self.data.append([None] * self.n_cols)

        else:
            open_pos = flat.index(None)
            ri = open_pos / self.n_cols
            # print ri
            if (ri + rows) > self.n_rows():
                logging.error("Error: DataTable row has more columns than labels: [%d, %d, %d]" % (ri, rows, self.n_rows()))
                for i in range((ri + rows) - self.n_rows()):
                    self.data.append([None] * self.n_cols)

        ri = open_pos / self.n_cols
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
