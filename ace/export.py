from .database import Database, Article, Table, Activation
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)

def export_database(db, filename, metadata=True, groups=False):

    res = ['id\tdoi\tx\ty\tz\tspace\tpeak_id\ttable_id\ttable_num']

    if metadata:
        res[0] += '\ttitle\tauthors\tyear\tjournal'
        
    if groups:
        res[0] += '\tgroups'

    articles = db.session.query(Article).filter(Article.tables.any()).all()
    for a in articles:
        logger.info('Processing article %s...' % a.id)
        for t in a.tables:
            for p in t.activations:
                if t.number is None: t.number = ''
                fields = [a.id, a.doi, p.x, p.y, p.z, a.space, p.id, t.id, t.number.strip('\t\r\n')]
                if metadata:
                    fields += [a.title, a.authors, a.year, a.journal]
                if groups:
                    if isinstance(p.groups, str):
                        p.groups = [p.groups]
                    elif p.groups is None:
                        p.groups = []
                    fields += ['///'.join(p.groups).encode('utf-8')]
                res.append('\t'.join(str(x) for x in fields))

    open(filename, 'w').write('\n'.join(res))