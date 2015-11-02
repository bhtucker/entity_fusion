# -*- coding: utf-8 -*-
"""
    traverse.py
    ~~~~~~~~~~~

    Logic for traversing broker clusters to company clusters
    Create views into the similarity / comp submission graph
    `output` table shows best broker name -> best company name

"""
from pprint import pprint


def recreate_aliases(session):

    stmt = """drop table if exists brokerage_aliases;
    drop table if exists broker_aliases;
    drop table if exists broker_clusters_with_ids;
    drop table if exists broker_cluster_comps;
    drop table if exists weighted_cluster_map;
    drop table if exists output;"""

    session.execute(stmt)
    session.commit()

    # find all companies' 'best name' (their own or their standard)
    stmt = """create table brokerage_aliases as (
            select name, standard
            from company_standardization
        union
            select ci.name, ci.name
        from company_importance ci
            left join company_standardization cs
            on ci.name = cs.name
        where cs.name is null)"""

    session.execute(stmt)
    session.commit()

    # find all brokers 'best name' (their own or their standard)
    # and list them with their original broker_ids to join to comps
    stmt = """create table broker_aliases as (
    select
        case when
            bd.standard is not null then bd.standard
        else b.name end as standard,
        b.realty_broker_id as member_id,
        b.name as name

    from broker_importance b
        left join broker_standardization bd
            on b.name = bd.name
        left join named_brokers nb
            on bd.standard = nb.name
        group by b.name, b.realty_broker_id, bd.standard, nb.realty_broker_id
    )"""

    session.execute(stmt)
    session.commit()

    # find all cluster members' comps, credit them to standard name
    stmt = """create table broker_cluster_comps as (
    select
        distinct on (bc.standard, comp_id)
        bc.standard,
        comp_id
    from
        broker_aliases bc
    join comp_broker_link cb on cb.realty_broker_id = bc.member_id
    )"""

    session.execute(stmt)
    session.commit()

    # provided count-weighted edges from 'best names'
    # of broker to company
    stmt = """create table weighted_cluster_map as (
    select
        bc.standard as broker_name,
        ga.standard as brokerage_name,
        count(*) as cnt
    from
        broker_cluster_comps bc
    join comp_brokerage_link cg on bc.comp_id = cg.comp_id
    join brokerages g on cg.realty_company_id = g.id
    join brokerage_aliases ga on g.name = ga.name
    group by bc.standard, ga.standard)"""

    session.execute(stmt)
    session.commit()

    # create 'best name map' by selecting the strongest-weighted brokerage
    # stored as an array to represent ties
    stmt = """create table output as (
    select
        m1.broker_name,
        array_agg(distinct m1.brokerage_name) as distinct_top_brokerages
    from weighted_cluster_map m1 join
            (select broker_name, max(cnt) as top_count
                from weighted_cluster_map
                group by broker_name) m2
            on
             m1.broker_name = m2.broker_name
        where m1.cnt = m2.top_count
        group by m1.broker_name)"""
    session.execute(stmt)
    session.commit()


def check_assignment_distribution(session):
    stmt = ("select array_length(distinct_top_brokerages, 1), count(*)"
            "from output group by array_length(distinct_top_brokerages, 1)"
            " order by count(*) DESC;")
    rv = session.execute(stmt)
    pprint("See that there are about the right number of clusters (~13k) "
           "and that the vast majority have a single best match."
           "Here is the count of brokers by number of brokerage assignments")
    pprint(list(rv))
    pprint("Check out demo.log for an example")
