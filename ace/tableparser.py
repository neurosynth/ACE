# coding: utf-8
  # use unicode everywhere

# import database
import regex  # Note: we're using features in the new regex module, not re!
import logging
from . import config
from .database import Activation, Table
from collections import Counter, defaultdict


logger = logging.getLogger(__name__)


def identify_standard_columns(labels):
    ''' Takes a set of column labels and returns an equal-length list with names
    of any standard columns detected. Unknown columns are assigned None.
    E.g., passing in ['p value', 'brain region', 'unknown_col'] would return
    ['p_value', 'region', None].
    '''
    standardized = [None] * len(labels)
    found_coords = False
    for i, lab in enumerate(labels):
        if regex.search('(^\s*ba$)|brodmann', lab):
            s = 'ba'
        elif regex.search('region|anatom|location|area', lab):
            s = 'region'
        elif regex.search('sphere|(^\s*h$)|^\s*hem|^\s*side', lab):
            s = 'hemisphere'
        elif regex.search('(^k$)|(mm.*?3)|volume|voxels|size|extent', lab):
            s = 'size'
        elif regex.match('\s*[xy]\s*$', lab):
            found_coords = True
            s = lab
        elif regex.match('\s*z\s*$', lab):
            # For z, we need to distinguish z plane from z-score.
            # Use simple heuristics:
            # * If no 'x' column exists, this must be a z-score
            # * If the preceding label was anything but 'y', must be a z-score
            # * Otherwise it's a z coordinate
            # Note: this could theoretically break if someone has non-contiguous
            # x-y-z columns, but this seems unlikely. If it does happen,
            # an alternative approach would be to check if the case of the 'z' column
            # matches the case of the 'x' column and make determination that
            # way.
            s = 'statistic' if not found_coords or labels[i - 1] != 'y' else 'z'
        elif regex.search('rdinate', lab):
            continue
        elif lab == 't' or regex.search('^(z|t).*(score|value)', lab):
            s = 'statistic'
        elif regex.search('p[\-\s]+.*val', lab):
            s = 'p_value'
        else:
            s = None
        standardized[i] = s
    return standardized


def identify_repeating_groups(labels):
    ''' Identify groups: any sets of columns where names repeat.
    Repeating groups must be contiguous; i.e., [x, y, z, w, x, y, z, f]
    will not match, but [w, f, x, y, z, x, y, z] will.

    Note that this will only handle one level of repetition; i.e., 
    hierarchical groupings will be ignored. E.g., in a 2 x 2 x 3 
    nesting of columns like hemisphere --> condition --> x/y/z, 
    only the 4 sets of repeating x/y/z columns will be detected.

    Returns a list of strings made up of the index of the first column
    in the group and the number of columns. E.g., '1/3' indicates the 
    group starts at the second column and contains 3 columns. These 
    keys can be used to directly look up names stored in a
    multicolumn_label dictionary.
    '''
    # OLD ALGORITHM: MUCH SIMPLER AND FASTER BUT DOESN'T WORK PROPERLY
    # FOR NON-CONTIGUOUS COLUMN GROUPS
    # target = '###'.join(unicode(x) for x in labels)
    # pattern = regex.compile(r'(.+?###.+?)(###\1)+')
    # matches = pattern.finditer(target)
    # groups = []
    # for m in matches:
    #     sp = m.span()
    #     n_cols_in_group = len(m.group(1).split('###'))
    #     start = len(target[0:sp[0]].split('###'))-1
    #     n_matches = len(m.group(0).split('###'))
    #     for i in range(n_matches/n_cols_in_group):
    #         groups.append('%d/%d' % ((i*n_cols_in_group)+start, n_cols_in_group))
    # return list(set(groups))

    groups = []
    n_labels = len(labels)
    label_counts = Counter(labels)
    rep_labels = set([k for k, v in list(label_counts.items()) if v > 1])
    # Track multi-label sequences. Key/value = sequence/onset
    label_seqs = defaultdict(list)

    # Loop over labels and identify any sequences made up entirely of labels with 
    # 2 or more occurrences in the list and without the starting label repeating.
    for i, lab in enumerate(labels):
        if lab not in rep_labels:
            continue
        current_seq = [lab]
        for j in range(i+1, n_labels):
            lab_j = labels[j]
            if lab_j not in rep_labels or lab_j == lab:
                break
            current_seq.append(lab_j)
        if len(current_seq) > 1:
            label_seqs['###'.join(current_seq)].append(i)

    # Keep only sequences that occur two or more times
    label_seqs = { k: v for k, v in list(label_seqs.items()) if len(v) > 1}
    
    # Invert what's left into a list where the sequence occurs at its start pos
    seq_starts = [None] * n_labels
    for k, v in list(label_seqs.items()):
        for start in v:
            seq_starts[start] = k.split('###')

    # Create boolean array to track whether each element has already been used
    labels_used = [False] * n_labels

    # Loop through labels and add a group if we find a sequence that starts at 
    # the current position and spans at least one currently unused cell.
    # This is necessary to account for cases where one sequence isn't always 
    # part of the same supersequence, e.g., the y/z in x/y/z could also be a 
    # part of a/y/z or b/y/z.
    for i, lab in enumerate(labels):
        if seq_starts[i] is not None:
            seq_size = len(seq_starts[i])
            if not all(labels_used[i:(i+seq_size)]):
                labels_used[i:(i+seq_size)] = [True] * seq_size

                # We need to make sure the group contains x/y/z information, 
                # otherwise we'll end up duplicating a lot of activations.
                # This is not a very good place to put this check; eventually
                # we need to refactor much of this class.
                groups.append('%d/%d' % (i, seq_size))

    return groups



def create_activation(data, labels, standard_cols, group_labels=[]):

    activation = Activation()

    for i, col in enumerate(data):

        # Cast to integer or float if appropriate
        # if regex.match('[-\d]+$', col):
        #     col = int(col)
        # elif regex.match('[-\d\.]+$', col):
        #     col = float(col)

        # Set standard attributes if applicable and do validation where appropriate.
        # Generally, validation will not prevent a bad value from making it into the
        # activation object, but it will flag any potential issues using the "problem" column.
        if standard_cols[i] is not None:

            sc = standard_cols[i]

            # Validate XYZ columns: Should only be integers (and possible trailing decimals).
            # If they're not, keep only leading numbers. The exception is that ScienceDirect 
            # journals often follow the minus sign with a space (e.g., - 35), which we strip.
            if regex.match('[xyz]$', sc):
                m = regex.match('(-)\s+(\d+\.*\d*)$', col)
                if m:
                    col = "%s%s" % (m.group(1), m.group(2))
                if not regex.match('(-*\d+)\.*\d*$', col):
                    logging.debug("Value %s in %s column is not valid" % (col, sc))
                    activation.problems.append("Value in %s column is not valid" % sc)
                    return activation
                col = (float(col))

            elif sc == 'region':
                if not regex.search('[a-zA-Z]', col):
                    logging.debug("Value in region column is not a string")
                    activation.problems.append("Value in region column is not a string")

            setattr(activation, sc, col)

        # Always include all columns in record
        activation.add_col(labels[i], col)
      
        # Handle columns with multiple coordinates (e.g., 45;12;-12).
        # Assume that any series of 3 numbers in a non-standard column
        # reflects coordinates. Will fail if there are leading numbers!!!
        # Also need to remove space between minus sign and numbers; some ScienceDirect
        # journals leave a gap.
        if not i in standard_cols:
            cs = '([\-\.\s]*\d{1,3}\.*\d{0,2})'
            m = regex.search('%s[,;\s]+%s[,;\s]+%s' % (cs, cs, cs), str(col).strip())
            if m:
                x, y, z = [regex.sub('-\s+', '-', c) for c in [m.group(1), m.group(2), m.group(3)]]
                logger.info("Found multi-coordinate column: %s\n...and extracted: %s, %s, %s" % (col, x, y, z))
                activation.set_coords(x, y, z)

    activation.groups = group_labels
    return activation


def parse_table(data):
    ''' Takes a DataTable as input and returns a Table instance. '''
    
    table = Table()
    n_cols = data.n_cols

    # Identify column names: first occurrence of unique (i.e. colspan=1) label.
      # Also track multi-column labels for group names.
    labels = [None] * n_cols
    multicol_labels = {}
    for i in range(data.n_rows):
        r = data[i]
        found_xyz = regex.search('\d+.*\d+.*\d+', '/'.join(r))  # use this later
        for j, val in enumerate(r):
            val = val.strip()
            # If a value is provided and the cell isn't an overflow cell (i.e., '@@'), and
            # there is no current label assigned to this column...
            if val != '' and not val.startswith('@@') and labels[j] is None:
                # Handle the first column separately, because first value in table
                # is often mistaken for label if label is left blank.
                # If all other labels have been found, or if there are lots of numbers
                # in the row, we must already be in contents, so assume the first row
                # denotes regions. Otherwise assume this is a regular column name.
                # Note: this heuristic is known to fail in the presence of multiple
                # unlabeled region columns. See e.g., CerCor bhl081, table 2.
                if j == 0 and (None not in labels[1::] or found_xyz):
                    labels[j] = 'region'
                else:
                    labels[j] = val
            else:
                # Store any multi-column labels. Key is the starting index and
                # colspan.
                m = regex.search('^@@(.*)@(\d+)$', val)
                if m:
                    multicol_labels["%d/%s" % (j, m.group(2))] = m.group(1)

    # Compact the list, although there shouldn't be any missing values at this point...
    # labels = [x.lower() for x in labels if x is not None]
    # Convert all labels to lowercase
    labels = [x.lower() if x is not None else '' for x in labels]
    n_cols = len(labels)

    # Sometimes tables have a single "Coordinates" column name
    # despite breaking X/Y/Z up into 3 columns, so we account for this here.
    for k, v in list(multicol_labels.items()):
        if regex.search('(ordinate|x.*y.*z)', v):
            st, span = k.split('/')
            start, end = int(st), (int(st) + int(span))
            if not regex.search('[a-zA-Z]', ''.join(labels[start:end])):
                logger.info(
                    "Possible multi-column coordinates found: %s, %s" % (k, v))
                labels[start:end] = ['x', 'y', 'z']

    # There shouldn't be any unfilled column labels at this point, but if there are,
    # log that information and skip table if flag is set.
    if None in labels:
        labels = [str(l) for l in labels]
        msg = 'Failed to identify at least one column label: [%s]. Skipping table!' % ', '.join(labels)
        if config.EXCLUDE_TABLES_WITH_MISSING_LABELS:
            logger.error(msg)
            return None
        else:
            logger.warning(msg)


    # Detect any standard column labels and any repeating column groups
    standard_cols = identify_standard_columns(labels)
    group_cols = identify_repeating_groups(labels)
    logger.debug("Labels: " + ', '.join(labels))
    logger.debug("Standard columns:" + ', '.join([str(x) for x in standard_cols if x is not None]))

    # Store a boolean list indicating which columns belong to a group
    cols_in_group = [False] * n_cols
    for g in group_cols:
        onset, length = [int(i) for i in g.split('/')]
        for i in range(onset, onset+length):
            cols_in_group[i] = True

    # Also store non-group labels for easy lookup later
    nongroup_labels = [l for (i,l) in enumerate(labels) if not cols_in_group[i]]

    # Loop over rows in table
    group_row = None
    activation_num = 0
    
    for r in data:
        logger.debug(r)

        n_cells = len(r)

        # Skip row if any value matches the column label--assume we're in header
        match_lab = False
        for i in range(n_cells):
            if r[i] == labels[i]: match_lab = True
        if match_lab: continue

        # If row is empty except for value in first column, assume the next few 
        # rows of coordinates are grouped together under this heading.
        # Note that this won't extract a hierarchical structure;
        # e.g., if there are two consecutive group-denoting rows,
        # the value in the second row will overwrite the one in the first row.
        if r[0] and not ''.join(r[1::]).strip():
            group_row = r[0].strip()
            continue

        # If first cell spans ALL columns, it probably also denotes a group, as we 
        # should already be past all header rows.
        if r[0].startswith('@@'):
            m = regex.search('@(\d+)', r[0])
            if int(m.group(1)) == n_cols:
                group_row = r[0].split('@')[2].strip()
                continue

        # Skip any additional header rows
        if n_cells != n_cols or regex.search('@@', ' '.join(r)): continue




      
        # If we don't have to worry about groups, the entire row is a single activation
        if not len(group_cols):
            activation = create_activation(r, labels, standard_cols, group_row)
            if activation.validate():
                table.activations.append(activation)

        # ...otherwise we need to iterate over groups and select appropriate columns for each.
        else:

            # Loop over groups and select appropriate columns
            for g in group_cols:
                onset, length = [int(i) for i in g.split('/')]
                # Get current grouping labels. Occasionally there are tables that have multiple 
                # groups of columns but don't attach separate high-order labels to them, so check
                # for the key first.
                groups = [multicol_labels[g]] if g in multicol_labels else []
                if group_row is not None: groups.append(group_row)
                group_specific_cols = list(range(onset, onset+length))

                # Select columns that belong to this activation: all columns that do not 
                # belong to any group, plus any columns that belong only to this group.
                activation_labels = []
                activation_columns = []
                activation_scs = [] # standard columns
                for (i,x) in enumerate(r):
                    if not cols_in_group[i] or i in group_specific_cols:
                        activation_labels.append(labels[i])
                        activation_columns.append(r[i])
                        activation_scs.append(standard_cols[i])
          
                # Create activation and add to table if it passes validation
                activation = create_activation(activation_columns, activation_labels, activation_scs, groups)
                if activation.validate():
                    table.activations.append(activation)

    table.finalize()
    return table if len(table.activations) else None



