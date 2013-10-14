import database
import logging

logger = logging.getLogger('ace')

def export_database(db, filename):

    res = ['id\tdoi\tx\ty\tz\tspace\tpeak_id\ttable_id']

    for a in db.articles:
    	logger.info('Processing article %s...' % a.id)
        for t in a.tables:
            for p in t.activations:
                res.append('\t'.join(str(x) for x in [
                           a.id, a.doi, p.x, p.y, p.z, a.space, p.id, t.id]))

    open(filename, 'w').write('\n'.join(res))
