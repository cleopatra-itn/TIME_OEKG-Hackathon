
import os
import json
import pandas as pd
import logging

from helper_functions import *

## My Graph
graph = "time"

## Clear/Reset Graph
clear_graph(graph)

uploadFileToOEKG(graph, "oekg_extension/graphs/euroscepticism.nt")

# create a graph
g = Graph()

# bind prefixes. Only used when you create triples in the .ttl format. But for the upload, we use .nt!
g.bind("oekg-r", OEKG_R)
g.bind("oekg-s", OEKG_S)
g.bind("so", SO)


def add_triples(docs, title_doc, text_doc, link_dict, sent_dict, cnt, topic):
    unq_urls = set()
    num_docs = 0
    num_events = 0
    num_entities = 0
    for i, doc_id in enumerate(docs):
        title = docs[doc_id]['title']
        publisher = site
        date = docs[doc_id]['date']
        day, month, year = date.split('/')
        if int(month) > 12:
            temp = month
            month = day
            day = temp
        
        date = '%s-%s-%s'%(year, month, day)

        link = link_dict[doc_id]
        if link in unq_urls:
            continue
        unq_urls.add(link)

        title_ents = title_doc[doc_id]
        text_ents = text_doc[doc_id]
        all_ents = title_ents+text_ents

        ## Get Unique Events and Entities from Title and Main body text
        unique_event_ids = set()
        unique_ent_ids = set()
        unique_ent_names = set()

        for ent in all_ents:
            wiki_id = ent['wd_id']

            oekg_id = getOEKGIdByWikidataId(wiki_id)

            for k, v in oekg_id.items():
                if k != 'error' and 'event' in v:
                    unique_event_ids.add(v)
                elif 'entity' in v:
                    unique_ent_ids.add(v)
                    unique_ent_names.add(ent['wd_label'])

        num_entities += len(unique_ent_ids)
        num_events += len(unique_event_ids)

        ## Add Triples into Graph
        g.add((OEKG_R.time_olymp_article_+str(cnt), RDF.type, SO.Article)) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), RDF.type, SO.Article))
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.url, URIRef(link))) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.url, URIRef(link)))
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.headline, Literal(title, lang))) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.headline, Literal(title, lang)))
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.publisher, Literal(publisher))) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.publisher, Literal(publisher)))
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.inLanguage, Literal(lang, datatype=XSD.language))) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.inLanguage, Literal(lang, datatype=XSD.language)))
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.datePublished, Literal(date, datatype=XSD.date))) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.datePublished, Literal(date, datatype=XSD.date)))

        ## Main Entity
        g.add((OEKG_R.time_olymp_article_+str(cnt), SO.mainEntity, URIRef(OEKG_R)+main_entity_id)) if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), SO.mainEntity, URIRef(OEKG_R)+main_entity_id))

        for event_id in unique_event_ids:
            g.add((OEKG_R.time_olymp_article_+str(cnt), SO.mentions, URIRef(OEKG_R)+event_id)) if topic == 'olympics' else \
                g.add((OEKG_R.time_euro_article_+str(cnt), SO.mentions, URIRef(OEKG_R)+event_id))
        for ent_id in unique_ent_ids:
            g.add((OEKG_R.time_olymp_article_+str(cnt), SO.mentions, URIRef(OEKG_R)+ent_id)) if topic == 'olympics' else \
                g.add((OEKG_R.time_euro_article_+str(cnt), SO.mentions, URIRef(OEKG_R)+ent_id)) 
        
        ## Add Sentiment
        g.add((OEKG_R.time_olymp_article_+str(cnt), ONYX.hasEmotionSet, OEKG_R.emotSet1))  if topic == 'olympics' else \
            g.add((OEKG_R.time_euro_article_+str(cnt), ONYX.hasEmotionSet, OEKG_R.emotSet1))
        g.add((OEKG_R.emotSet1, ONYX.hasEmotion, OEKG_R.emot1))
        sent_sc = sent_dict[doc_id]
        if sent_sc <= -0.5:
            g.add((OEKG_R.emot1, ONYX.hasEmotionCategory, WNA.negative_emotion))
        elif sent_sc >= 0.5:
            g.add((OEKG_R.emot1, ONYX.hasEmotionCategory, WNA.positive_emotion))
        else:
            g.add((OEKG_R.emot1, ONYX.hasEmotionCategory, WNA.neutral_emotion))
        g.add((OEKG_R.emot1, ONYX.hasEmotionIntensity, Literal(sent_sc, datatype=XSD.double)))

        cnt += 1
        num_docs += 1

    return cnt, num_docs, num_events, num_entities


## Add Euroscepticism articles
cnt = 0
for site in ['elpais', 'elmundo', 'theguardian', 'dailymail']:
    print(site)
    if site in ['dailymail', 'theguardian', 'bbc', 'telegraph']:
        lang = 'en'
    elif site in ['folha', 'globo', 'estadao']:
        lang = 'pt'
    elif site in ['elpais', 'elmundo']:
        lang = 'es'
    else:
        logging.error(f"Unsupported language {lang}. Please use [en, pt, es]!")

    docs = json.load(open('data/scraped_docs/json_data_daniela/%s_allnews.json'%(site), 'r'))
    title_doc = json.load(open('title_entity_data/%s_entities.json'%(site)))
    text_doc = json.load(open('text_entity_data/%s_entities.json'%(site)))
    data_csv = pd.read_csv('data/raw_docs/news_docs_daniela/google/%s_final.txt'%(site), delimiter='\t')
    sentiment_csv = pd.read_csv('sentiment_output/%s_sentistrength.csv'%(site), delimiter='\t')

    main_entity_id = 'entity_99999999'

    link_dict = {}
    for i, row in data_csv.iterrows():
        link_dict['rank_%s'%(row['rank'])] = row['link']

    sent_dict = {}
    for i, row in sentiment_csv.iterrows():
        sent_dict['rank_%s'%(row['Rank'])] = row['Title Polarity Score']

    cnt, num_docs, num_events, num_entities = add_triples(docs, title_doc, text_doc, link_dict, sent_dict, cnt, 'euroscepticism')

    print('Number of Articles: %d'%(num_docs))
    print('Number of Events: %d'%(num_events))
    print('Number of Entities: %d'%(num_entities))


## Add Olympics Articles
cnt = 0
for site in ['bbc', 'telegraph', 'theguardian', 'dailymail', 'estadao', 'folha', 'globo']:
    if site in ['dailymail', 'theguardian', 'bbc', 'telegraph']:
        lang = 'en'
    elif site in ['folha', 'globo', 'estadao']:
        lang = 'pt'
    elif site in ['elpais', 'elmundo']:
        lang = 'es'
    else:
        logging.error(f"Unsupported language {lang}. Please use [en, pt, es]!")
    
    for city in ['london', 'rio']:
        print(site, city)
        docs = json.load(open('data/scraped_docs/json_data_caio/%s_%s_allnews.json'%(city, site), 'r'))
        title_doc = json.load(open('title_entity_data/%s_%s_entities.json'%(city, site)))
        text_doc = json.load(open('text_entity_data/%s_%s_entities.json'%(city, site)))
        data_csv = pd.read_csv('data/raw_docs/news_docs_caio/google/%s_%s_final.txt'%(city, site), delimiter='\t')
        sentiment_csv = pd.read_csv('sentiment_output/%s_%s_sentistrength.csv'%(city, site), delimiter='\t')
        main_entity_id = 'event_981207' if city == 'london' else 'event_981215'
        
        link_dict = {}
        for i, row in data_csv.iterrows():
            link_dict['rank_%s'%(row['rank'])] = row['link']

        sent_dict = {}
        for i, row in sentiment_csv.iterrows():
            sent_dict['rank_%s'%(row['Rank'])] = row['Title Polarity Score']


        cnt, num_docs, num_events, num_entities = add_triples(docs, title_doc, text_doc, link_dict, sent_dict, cnt, 'olympics')

        print('Number of Articles: %d'%(num_docs))
        print('Number of Events: %d'%(num_events))
        print('Number of Entities: %d'%(num_entities))


# Write triples into a file "example_articles.nt". Use the .nt format for uploading the graph.
# For testing, you can use "turtle" as well.
filename = "oekg_extension/graphs/time_articles.nt"
file = open(filename, "w")
file.write(g.serialize(format='nt').decode("utf-8"))
file.close()

uploadFileToOEKG(graph, "oekg_extension/graphs/time_articles.nt")


