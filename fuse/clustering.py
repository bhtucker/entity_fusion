# -*- coding: utf-8 -*-
"""
    clustering.py
    ~~~~~~~~~~~~~

    Logic for operating on trigrams and writing standardization links
"""
import math
from collections import defaultdict, Counter
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import column
from fuzzywuzzy import fuzz

SIMILAR_ENTITY_TABLE_MAP = {
    'company': 'similar_companies',
    'broker': 'similar_brokers'
}

BAD_TRIGRAMS = set(['"  c"',
                    '"  e"',
                    '"  p"',
                    '"  r"',
                    '" co"',
                    '" es"',
                    '" pr"',
                    '" re"',
                    '"al "',
                    '"es "',
                    '"te "',
                    'ani',
                    'ate',
                    'cia',
                    'com',
                    'eal',
                    'erc',
                    'ert',
                    'est',
                    'ial',
                    'ies',
                    'mer',
                    'mme',
                    'mpa',
                    'nie',
                    'omm',
                    'omp',
                    'ope',
                    'pan',
                    'per',
                    'pro',
                    'rci',
                    'rea',
                    'rop',
                    'rti',
                    'sta',
                    'tat',
                    'tie',
                    "  i",
                    " in",
                    'inc',
                    "nc "])


def setup_db(session):
    stmt = ("create extension pg_trgm; "
            "CREATE INDEX named_brokers_trgm_idx ON named_brokers USING gin (name gin_trgm_ops); "
            "CREATE INDEX brokerages_trgm_idx ON brokerages USING gin (name gin_trgm_ops); ")
    session.execute(stmt)
    session.commit()


def setup_similarities(session):
    """
    Define entity name importance by count of comps
    and potentially similar entities via pg_trgm 
    """

    stmt = """create table company_importance as (
        select
            name, count(distinct cg.comp_id)
        from brokerages b join comp_brokerage_link cg
            on b.id = cg.realty_company_id
        group by name
        )"""
    session.execute(stmt)
    session.commit()

    stmt = """create table broker_importance as (
        select
            b.realty_broker_id, name, count(distinct cb.comp_id)
        from named_brokers b join comp_broker_link cb
            on b.realty_broker_id = cb.realty_broker_id
        group by name, b.realty_broker_id)"""
    session.execute(stmt)
    session.commit()

    stmt = """
    CREATE INDEX relevant_brokerage_trgm_idx ON company_importance USING gist (name gist_trgm_ops);
    CREATE INDEX broker_importance_trgm_idx ON broker_importance USING gist (name gist_trgm_ops);
    select set_limit(.7);
    """
    session.execute(stmt)
    session.commit()

    stmt = """create table similar_brokers as (
        select
            distinct b1.name as name_1,
            b2.name as name_2,
            similarity(b1.name, b2.name)
        from
            broker_importance b1 join broker_importance b2
        on b1.name != b2.name
        and b1.name % b2.name)"""
    session.execute(stmt)
    session.commit()

    stmt = """create table similar_companies as (
        select
            distinct b1.name as name_1,
            b2.name as name_2,
            similarity(b1.name, b2.name)
        from
            company_importance b1 join company_importance b2
        on b1.name != b2.name
        and b1.name % b2.name)"""
    session.execute(stmt)
    session.commit()


"""Creating and resolving clusters
Tasks -> functions used:
    discover near matches -> bucket_by_trigram_signature
    validate members are truly similar -> bucket_by_trigram_signature
    select a representative element to be the standard -> select_standard_name
    update other data to use the standard -> link_members_to_standard
"""


def bucket_by_trigram_signature(session, similarity_table_name, minimum_similarity=None):
    """
    Query and logic for trigram signature grouping
    """

    stmt = """select name_1, show_trgm(
        replace( replace(name_1, '"', ''), '$ ', '')),
    name_2, show_trgm(
        replace( replace(name_2, '"', ''), '$ ', '')),
     similarity from {table}
    where similarity > {similarity}""".format(
        table=similarity_table_name,
        similarity=minimum_similarity or .7)
    rv = session.execute(stmt)
    res = list(rv)

    # use only the 10 - 99 percentile of trigrams to limit outlier noise
    trigram_counts = Counter([y for x in res for y in x[1] + x[3]])

    sorted_counts = trigram_counts.most_common()
    n_trigrams = len(sorted_counts)
    n_too_high = n_trigrams - int(math.floor(n_trigrams * .99))
    n_too_low = int(math.floor(n_trigrams * .1))

    # these trigrams are in the frequency range we consider to be 'signal'
    valid_trigrams = set([x[0] for x in sorted_counts[n_too_high: -n_too_low]])

    # these are highly-frequent trigrams that we shouldn't overly rely on
    top_trigrams = set(
        [x[0] for x in sorted_counts[:int(math.floor(n_too_high * 1.5))]])

    # assign names to clusters based on their shared common trigrams
    clusters = defaultdict(set)

    for row in res:

        name_1 = row[0]
        name_2 = row[2]

        # some light ad hoc data quality improvers when trigrams seem weak:
        # use a non-trigram string similarity measure
        mostly_common = False
        few_distinctions = False
        shared_trigrams = set(row[1]).intersection(set(row[3]))

        if float(
            len(shared_trigrams.intersection(top_trigrams))
        ) / len(shared_trigrams) > .6:
            mostly_common = True

        key = list(shared_trigrams.intersection(valid_trigrams))

        # on average signatures share >10 trigrams, so 5 or fewer is bad
        if len(key) < 6:
            few_distinctions = True
        if few_distinctions or mostly_common:
            clean_name_1, clean_name_2 = name_1, name_2
            for gram in BAD_TRIGRAMS:
                clean_name_1 = clean_name_1.replace(gram, '')
            if fuzz.WRatio(clean_name_1, clean_name_2) < 90:
                # unless these still look similar de-noised, skip this pair
                continue
        key.sort()
        string_key = ','.join(key)
        clusters[string_key].add(name_1)
        clusters[string_key].add(name_2)
    return clusters


def select_standard_name(session, cluster, importance_table_name):
    """
    Use cluster members for a WHERE ... IN (...) query
    Use SQLAlchemy to handle the escaping
    """
    stmt = session.query('name from %s' % importance_table_name) \
        .filter(column('name').in_(list(cluster))) \
        .order_by('"count" DESC') \
        .limit(1)
    rv = session.execute(stmt)
    res = list(rv)
    return res[0][0]


def initialize_link_table(session, link_table_name):
    stmt = """CREATE TABLE if not exists {table}
    (name text, standard text)
    """.format(table=link_table_name)
    session.execute(stmt)
    session.commit()


def link_members_to_standard(session, cluster, standard, link_table):
    """
    Insert links for each cluster member
    Use SQLAlchemy to handle the escaping
    """
    for member in cluster:
        stmt = link_table.insert(dict(name=member, standard=standard))
        session.execute(stmt)
    session.commit()


def create_links_for_cluster_collection(session, entity, minimum_similarity=None):
    """
    Main entry point to cluster creation
    Assumes existence of similiarity and importance tables (via setup_company_similarities)
    Creates 'entity_standardization' table to map members to standards
    """
    metadata = MetaData(bind=session.get_bind())

    similarity_table_name = SIMILAR_ENTITY_TABLE_MAP[entity]
    importance_table_name = entity + '_importance'
    link_table_name = entity + '_standardization'
    initialize_link_table(session, link_table_name)
    metadata.reflect()
    [link_table] = [
        x for x in metadata.sorted_tables
        if x.name == link_table_name]

    clusters = bucket_by_trigram_signature(session,
                                           similarity_table_name, minimum_similarity=minimum_similarity)
    for cluster in clusters.values():
        standard = select_standard_name(
            session, cluster, importance_table_name)
        link_members_to_standard(session, cluster, standard, link_table)
