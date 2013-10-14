import database
import logging

logger = logging.getLogger('ace')

def export_database(db, filename, metadata=True):

    res = ['id\tdoi\tx\ty\tz\tspace\tpeak_id\ttable_id\ttable_num']

    if metadata:
    	res[0] += '\ttitle\tauthors\tyear\tjournal'

    for a in db.articles:
    	logger.info('Processing article %s...' % a.id)
        for t in a.tables:
            for p in t.activations:
            	fields = [a.id, a.doi, p.x, p.y, p.z, a.space, p.id, t.id, t.number]
            	if metadata:
            		fields += [a.title, a.authors, a.year, a.journal]
                res.append('\t'.join(str(x) for x in fields))

    open(filename, 'w').write('\n'.join(res))
