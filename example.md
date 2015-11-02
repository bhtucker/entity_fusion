
# Example Walkthrough


##Overview
The input data are the names of real estate brokers, the deals they've participated in, the brokerage IDs associated with these deals, and the names of these brokerages.

Below I'll show how you

##The Problem

Consider the deals submitted by Armand Tiano:

```
bhtucker$ grep Tiano realty_broker.csv | grep Armand
3399,0,"Armand Tiano"
3660,0,"Armand J. Tiano"
10303,0,"Terry Deveau, Armand Tiano"
10543,0,"Terry Deveau/Armand Tiano"
10587,0,"Jay Phillips, Armand Tiano"
12133,0,"Armand Tiano, Terry Deveau"
31351,0,"Kevin Delehanty, Jay Phillips, Armand Tiano, Luke Allard"
```

He exists across several broker_ids, with multiple spellings and multi-broker entities

I flatten these multi-broker ids out as an ingestion step, then can view all of his associated deals somewhat simply:

```
In [643]: broker_comps[broker_comps['name'].map(lambda v: 'Tiano' in v)]
Out[643]: 
         comp_id  realty_broker_id     id             name  version
comp_id                                                            
107694    107694              3660   3660  Armand J. Tiano        0
159364    159364              3660   3660  Armand J. Tiano        0
139987    139987              3399   3399     Armand Tiano        0
149008    149008              3399   3399     Armand Tiano        0
199717    199717              3399   3399     Armand Tiano        0
199727    199727              3399   3399     Armand Tiano        0
288875    288875              3399   3399     Armand Tiano        0
334974    334974              3399   3399     Armand Tiano        0
339919    339919              3399   3399     Armand Tiano        0
```

Here we see that there are 9 deals associated with Tiano. By glancing at the realty_broker_id column, we see that his multi-person broker IDs are not associated with any comps, so in this case the fact that we've flattened these out will not matter to us. 

Because comps are driving linkage from brokers to firms, I'm only considering names attributed to deals in this dataset. I could (but didn't) use these multi-broker ID records to infer who Armand's colleagues are and add them to the list.


##String Clustering
From the set of single-person broker names that were associated with comps, I grouped these names based on their trigram signature. 

```
In [644]: clusters = bucket_by_trigram_signature(session, 'similar_brokers')
In [645]: filter(lambda (k, v): 'Armand Tiano' in str(v), clusters.iteritems())
Out[645]: 
[(u'  t, ar, ti,and,ano,arm,ian,man,nd ,no ,rma',
  {u'Armand J. Tiano', u'Armand Tiano'})]
```

I selected the one of these names associated with the most deals to be the 'standard' name:

```
comps=# select * from broker_standardization where name like 'Armand%Tiano';
      name       |   standard   
-----------------+--------------
 Armand Tiano    | Armand Tiano
 Armand J. Tiano | Armand Tiano
(2 rows)
```

Then I found all the comps associated with this cluster and attributed them to the standard name:

```
comps=# select * from broker_cluster_comps where standard like 'Armand%Tiano';
   standard   | comp_id 
--------------+---------
 Armand Tiano |  107694
 Armand Tiano |  139987
 Armand Tiano |  149008
 Armand Tiano |  159364
 Armand Tiano |  199717
 Armand Tiano |  199727
 Armand Tiano |  288875
 Armand Tiano |  334974
 Armand Tiano |  339919
(9 rows)
```

As you can see, these comps are associated with numerous brokerages, all with their own inconsistencies. Some deals are also missing brokerage links, and these are eventually omitted as well:

```
comps=# select * from broker_cluster_comps bc 
# join comp_brokerage_link cg
# on bc.comp_id = cg.comp_id
# join brokerages g
# on cg.realty_company_id = g.id
# where standard like 'Armand%T%';

   standard   | comp_id | comp_id |             name               
--------------+---------+--------+---------------------------------
 Armand Tiano |  199717 |  199717| Cornish & Carey Commercial
 Armand Tiano |  199727 |  199727| Cornish & Carey
 Armand Tiano |  159364 |  159364| Cornish & Carey
 Armand Tiano |  107694 |  107694| Cornish & Carey
 Armand Tiano |  139987 |  139987| Cornish & Carey Commercial Clie
 Armand Tiano |  339919 |  339919| Newmark Cornish & Carey
 Armand Tiano |  334974 |  334974| Newmark Cornish & Carey
 Armand Tiano |  288875 |  288875| Newmark Cornish & Carey
 Armand Tiano |  149008 |        | 

```


##From Aliases to Entities
We can aggregate these links under each brokerage's "best name" and get a weighted edge from a broker entity to various brokerage entities:


```
comps=# select * from weighted_cluster_map where broker_name like 'Armand%Tiano';
 broker_name  |       brokerage_name       | cnt 
--------------+----------------------------+-----
 Armand Tiano | Newmark Cornish & Carey    |   3
 Armand Tiano | Cornish & Carey            |   5
 Armand Tiano | Cornish & Carey Commercial |   1
(3 rows)
```

Here we see a potential problem in this sketch implementation of trigram signatures: Armand's 8 deals have turned into 9! Because our signatures are based on the common trigrams of the two names being compared, a common name can be linked into two different groups over the course of two different comparisons. We see this in the standardization links table: 

```
comps=# select * from company_standardization  where name = 'Cornish & Carey Commercial';
            name            |          standard          
----------------------------+----------------------------
 Cornish & Carey Commercial | Cornish & Carey Commercial
 Cornish & Carey Commercial | Cornish & Carey
(2 rows)
```

The two cluster link entries for `Cornish & Carey Commercial` lead it to be counted towards the weight for each of those brokerage clusters in the `weighted_cluser_map`.
Since this captures the uncertainty regarding the correct standard for this member name, it is somewhat defensible. But the overweighting of these less-certain cluster members is an undesirable trait. I would likely revise this in future use.

Finally, we attribute the top-weighted brokerage cluster as Armand's employer:

```
comps=# select * from output where broker_name like 'Armand%Tiano';
 broker_name  | distinct_top_brokerages 
--------------+-------------------------
 Armand Tiano | {"Cornish & Carey"}
(1 row)
```

In fact, had we not also included the two deals by Armand J. Tiano that were linked to "Cornish & Carey", we wouldn't have identified "Cornish & Carey" as a better firm name for Tiano than "Newmark Cornish & Carey", so the clustering on both sides contributes to solution quality.


##Potential revisions:

* Trigrams are a good general purpose grouping tool, but when used alone they are not sensitive to various other informative features. For instance, identifying low-information "real estate" words or very common first names is not done in a programmatic way here. Further, whether two broker names are associated with similar brokerages was not incorporated into their name groupings (or the inverse re: brokerage names appearing to have similar staff).
* Information about the comps themselves would also improve the system: the barrier to linking a Staten-Island-only name to a Queens-only name should be higher than names associated with similar deals.
* A richer model of name entries (for instance, a table recording a brokerage's original name and id, as well as various cleaned versions of the name string and cluster ids) would have been useful. 
* As described above, only deal-linked entities were considered. Much information was discarded that way.
* About 10% of brokers were ultimately linked with >1 brokerage. If a single match is a hard requirement, chronology information about comps could help select the most likely current firm, or the most popular could simply be deemed the winner.



