from .database import Article
from sqlalchemy import func, or_
import logging
import csv
from pathlib import Path
import datetime
import json

logger = logging.getLogger(__name__)

def export_database(db, foldername, skip_empty=True):
    # Create folder if it doesn't exist
    foldername = Path(foldername)
    foldername.mkdir(parents=True, exist_ok=True)

    article_columns = ['pmid', 'doi', 'authors', 'title', 'journal', 'publication_year', 'coordinate_space']
    art_results = []

    coordinate_columns = ['pmid', 'table_id', 'table_label', 'table_caption', 'table_number', 
        'x', 'y', 'z', 'p_value', 'region', 'size', 'statistic', 'groups']
    coordinates = []

    text_columns = ['pmid', 'title' ,'abstract', 'body']
    texts = []

    nv_colls_col = ['pmid','collection_id']
    nv_colls = []

    nv_images_col = ['pmid','image_id']
    nv_images = []

    articles = db.session.query(Article)
    if skip_empty:
        articles = articles.filter(or_(Article.tables.any(), Article.neurovault_links.any()))

    for art in articles:
        logger.info('Processing article %s...' % art.id)

        art_results.append([art.id, art.doi, art.authors, art.title, art.journal, art.year, art.space])
        texts.append([art.id, art.title, art.abstract, art.text])

        for t in art.tables:
            for p in t.activations:
                if t.number is None: t.number = ''
                if isinstance(p.groups, str):
                    p.groups = [p.groups]
                elif p.groups is None:
                    p.groups = []
                groups = '///'.join(p.groups)

                coordinates.append([art.id, t.id, t.label, t.caption, t.number, 
                    p.x, p.y, p.z, p.p_value, p.region, p.size, p.statistic, groups])
            
        for nv in art.neurovault_links:
            if nv.type == 'collection':
                nv_colls.append([art.id, nv.neurovault_id])
            elif nv.type == 'image':
                nv_images.append([art.id, nv.neurovault_id])

    # Save articles as tab separated file
    with (foldername / 'metadata.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(article_columns)
        writer.writerows(art_results)

    # Save coordinates as tab separated file
    with (foldername / 'coordinates.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(coordinate_columns)
        writer.writerows(coordinates)

    # Save texts as tab separated file
    with (foldername / 'text.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(text_columns)
        writer.writerows(texts)

    # Save NV links
    with (foldername / 'neurovault_collections.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(nv_colls_col)
        writer.writerows(nv_colls)

    with (foldername / 'neurovault_images.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(nv_images_col)
        writer.writerows(nv_images)

    # Save json file with time of export
    export_md = {
        "exported": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "n_articles": len(art_results),
        "n_activations": len(coordinates),
        "n_nv_collections": len(nv_colls),
        "n_nv_images": len(nv_images)

    }

    with (foldername / 'export.json').open('w') as f:
        json.dump(export_md, f)