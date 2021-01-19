import os
import json
import logging
import time

import urllib.parse
from urllib.request import Request

import spacy

from named_entity_linking.nerd import link_annotations, fix_entity_types

import argparse

parser = argparse.ArgumentParser(description='Parse documents and extract entities')
parser.add_argument('--topic', type=str, default='olympic')
parser.add_argument('--site', type=str, default='bbc')
parser.add_argument('--city', type=str, default='london')

args = parser.parse_args()

def get_spacy_annotations(text, spacy_ner):
    doc = spacy_ner(text)
    named_entities = []
    for ent in doc.ents:
        named_entities.append({
            'text': ent.text,
            'type': ent.label_,
            'start': ent.start_char,
            'end': ent.end_char,
            'cms': None,
        })
    return named_entities


def get_wikifier_annotations(text, language):
    threshold = 1.0
    endpoint = 'http://www.wikifier.org/annotate-article'
    language = language
    key = 'vnsxnxbclshssfdjkrkxfdqvhpadks'
    wikiDataClasses = 'false'
    wikiDataClassIds = 'true'
    includeCosines = 'false'
    data = urllib.parse.urlencode([("text", text), ("lang", language), ("userKey", key),
                                   ("pageRankSqThreshold", "%g" % threshold), ("applyPageRankSqThreshold", "true"),
                                   ("nTopDfValuesToIgnore", "200"), ("nWordsToIgnoreFromList", "200"),
                                   ("wikiDataClasses", wikiDataClasses), ("wikiDataClassIds", wikiDataClassIds),
                                   ("support", "true"), ("ranges", "false"), ("includeCosines", includeCosines),
                                   ("maxMentionEntropy", "3")])

    req = urllib.request.Request(endpoint, data=data.encode("utf8"), method="POST")
    with urllib.request.urlopen(req, timeout=60) as f:
        response = f.read()
        response = json.loads(response.decode("utf8"))
        if 'annotations' in response:
            return {'processed': True, 'annotations': response['annotations']}
        else:
            return {'processed': False, 'annotations': []}


topic = args.topic
site = args.site
city = args.city

if site in ['dailymail', 'theguardian', 'bbc', 'telegraph']:
    lang = 'en'
    spacy_ner = spacy.load("en_core_web_lg")
elif site in ['folha', 'globo', 'estadao']:
    lang = 'pt'
    spacy_ner = spacy.load("pt_core_news_lg")
elif site in ['elpais', 'elmundo']:
    lang = 'es'
    spacy_ner = spacy.load("es_core_news_lg")
else:
    logging.error(f"Unsupported language {lang}. Please use [en, pt, es]!")


event_list = set(open('oekg_extension/named_entity_linking/eventKG.csv').read())

if topic == 'euro':
    docs = json.load(open('data/scraped_docs/json_data_daniela/%s_allnews.json'%(site), 'r'))
else:
    docs = json.load(open('data/scraped_docs/json_data_caio/%s_%s_allnews.json'%(city, site), 'r'))


entity_dict = {}
for i, doc_id in enumerate(docs):
    print(i)

    title = docs[doc_id]['title']
    text = docs[doc_id]['text']

    ## Only titles for now
    spacy_annotations = get_spacy_annotations(text, spacy_ner)
    wikifier_annotations = get_wikifier_annotations(text, language=lang)

    linked_entities = link_annotations(spacy_annotations, wikifier_annotations) 
    linked_entities = fix_entity_types(linked_entities, event_list=event_list)

    entity_dict[doc_id] = linked_entities

    time.sleep(0.5)


if topic == 'euro':
    json.dump(entity_dict, open('text_entity_data/%s_entities.json'%(site), 'w', encoding='utf-8'))
else:
    json.dump(entity_dict, open('text_entity_data/%s_%s_entities.json'%(city, site), 'w', encoding='utf-8'))
