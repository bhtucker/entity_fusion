# -*- coding: utf-8 -*-
"""
    run.py
    ~~~~~~

    Procedural code to run clustering on a fresh db
"""

from csv_io import ingest_csvs
from clustering import (
    SIMILAR_ENTITY_TABLE_MAP,
    setup_db,
    setup_similarities,
    create_links_for_cluster_collection)
from traverse import (
    recreate_aliases,
    check_assignment_distribution)


if __name__ == '__main__':
    session = ingest_csvs()
    setup_db(session)
    setup_similarities(session)
    for entity in SIMILAR_ENTITY_TABLE_MAP:
        create_links_for_cluster_collection(session, entity)
    recreate_aliases(session)
    check_assignment_distribution(session)
