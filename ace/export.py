from .database import Article
from sqlalchemy import or_
import logging
import csv
from pathlib import Path
import datetime
import json
from tqdm import tqdm

logger = logging.getLogger(__name__)

def export_database(db, foldername, skip_empty=True, table_html=False):
    # Create folder if it doesn't exist
    foldername = Path(foldername)
    foldername.mkdir(parents=True, exist_ok=True)

    article_columns = [
        'pmid', 'doi', 'authors', 'title', 'journal',
        'publication_year', 'coordinate_space'
    ]
    art_results = []

    coordinate_columns = [
        'pmid', 'table_id', 'table_label', 'x', 'y', 'z',
        'p_value', 'region', 'size', 'statistic', 'groups'
    ]
    coordinates = []

    # New table.csv columns
    table_columns = [
        'pmid', 'table_id', 'table_label', 'table_caption',
        'table_foot', 'n_header_rows', 'table_raw_file'
    ]
    tables_data = []

    text_columns = ['pmid', 'title', 'abstract', 'body']
    texts = []

    nv_colls_col = ['pmid', 'collection_id']
    nv_colls = []

    nv_images_col = ['pmid', 'image_id']
    nv_images = []

    print("Exporting database to %s" % foldername)

    articles = db.session.query(Article)
    if skip_empty:
        articles = articles.filter(
            or_(Article.tables.any(), Article.neurovault_links.any())
        )

    for art in tqdm(articles):
        art_results.append([
            art.id, art.doi, art.authors, art.title,
            art.journal, art.year, art.space
        ])
        texts.append([art.id, art.title, art.abstract, art.text])

        for t in art.tables:
            # Prepare table data row
            table_foot = t.footnotes if hasattr(t, 'footnotes') else ''
            n_header_rows = t.header_rows if hasattr(t, 'header_rows') else 1
            table_raw_file = (
                f"tables/{art.id}/{t.id}.html"
                if table_html
                else ''
            )
            
            tables_data.append([
                art.id,  # Using PMID as pmcid for now
                t.id,
                t.label,
                t.caption,
                table_foot,
                n_header_rows,
                table_raw_file
            ])
            
            for p in t.activations:
                if isinstance(p.groups, str):
                    p.groups = [p.groups]
                elif p.groups is None:
                    p.groups = []
                groups = '///'.join(p.groups)

                # Only include specified fields for coordinates
                coordinates.append([
                    art.id,
                    t.id,
                    t.label,
                    p.x,
                    p.y,
                    p.z,
                    p.p_value,
                    p.region,
                    p.size,
                    p.statistic,
                    groups
                ])
            
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
    
    # Save table data as CSV
    with (foldername / 'tables.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(table_columns)
        writer.writerows(tables_data)

    # Save texts as tab separated file
    with (foldername / 'text.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(text_columns)
        writer.writerows(texts)

    # Save NV links
    with (foldername / 'neurovault_collections.csv').open(
        'w', newline=''
    ) as f:
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
        "n_tables": len(tables_data),
        "n_nv_collections": len(nv_colls),
        "n_nv_images": len(nv_images)
    }

    with (foldername / 'export.json').open('w') as f:
        json.dump(export_md, f)

    if table_html:
        # Save table HTML files if available
        tables_dir = foldername / 'tables'
        tables_dir.mkdir(parents=True, exist_ok=True)
        
        for art in articles:
            art_dir = tables_dir / str(art.id)
            art_dir.mkdir(parents=True, exist_ok=True)
            
            for t in art.tables:
                if t.input_html:
                    table_file = art_dir / f"{t.id}.html"
                    with table_file.open('w', encoding='utf-8') as f:
                        f.write(t.input_html)