# -*- coding: utf-8 -*-
"""
    csv_io.py
    ~~~~~

    Read CSVs, make initial tables
"""

from sqlalchemy import engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from fuzzywuzzy.utils import asciidammit

SQLALCHEMY_DATABASE_URI = 'postgresql://localhost/comps2'


def get_brokers():
    broker_raw = map(
        lambda s: s.strip().split(','),
        open('realty_broker.csv', 'r').readlines()
    )
    brokers = []
    for row in broker_raw:
        if len(row) < 3:
            continue
        for broker in row[2:]:
            broker = asciidammit(broker.replace('"', '').strip())
            try:
                brokers.append(
                    dict(realty_broker_id=int(row[0]),
                         version=int(row[1]),
                         name=broker))
            except:
                pass
    brokers_df = pd.DataFrame(brokers)
    return brokers_df


def ingest_csvs():
    _engine = engine.create_engine(SQLALCHEMY_DATABASE_URI)
    session = sessionmaker(bind=_engine)()

    company = pd.read_csv('realty_company.csv')
    company['name'] = company['name'].map(asciidammit)
    company.to_sql('brokerages', _engine)

    comp_company = pd.read_csv('comp_master_tenant_realty_company.csv')
    comp_company.to_sql('comp_brokerage_link', _engine)

    comp_broker = pd.read_csv('comp_master_tenant_realty_broker.csv')
    comp_broker.to_sql('comp_broker_link', _engine, chunksize=1000)

    brokers_df = get_brokers()
    brokers_df.to_sql('named_brokers', _engine, chunksize=1000)

    return session
