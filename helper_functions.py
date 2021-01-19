import requests
import urllib.parse
import numpy as np
import math
from jsonmerge import merge

from rdflib import URIRef, BNode, Literal, Namespace, Graph, XSD
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
    PROF, PROV, RDF, RDFS, SH, SKOS, SOSA, SSN, TIME, \
    VOID, XMLNS, XSD

SO = Namespace('http://schema.org/')
OEKG_R = Namespace(u'http://oekg.l3s.uni-hannover.de/resource/')
OEKG_S = Namespace('http://oekg.l3s.uni-hannover.de/schema/')
SEM = Namespace('http://semanticweb.cs.vu.nl/2009/11/sem/')
ONYX = Namespace('http://www.gsi.dit.upm.es/ontologies/onyx/ns#')
WNA = Namespace("http://www.gsi.dit.upm.es/ontologies/wnaffect/ns#")
url = "http://smldapi.l3s.uni-hannover.de/" # This needs to be changed


def uploadFileToOEKG(graph, file_name):
    print("uploadFileToOEKG: " + url + "upload/"+graph)
    files = {'upload_file': open(file_name, 'rb')}
    r = requests.post(url + "upload/"+graph, files=files)#, data=data)
    print(r.text)


def getOEKGIdByWikidataId(wikidata_id):
    # Get OEKG ID of an entity via its Wikidata ID
    return requests.get(url + "api/wikidataId/" + wikidata_id).json()


def getOEKGIdsByWikidataIds(*wikidata_ids):
    # Get OEKG IDs of a set of entity via its Wikidata ID
    ids = {}
    for ids_sublist in np.array_split(wikidata_ids, math.ceil(len(wikidata_ids) / 5000)):
        res = requests.post(url + "api/wikidataIds", data= ";".join(ids_sublist)).json()
        ids = merge(ids, res)
    return ids


def getOEKGIdByWikipediaId(language, wikipedia_id):
    # Get OEKG ID of an entity via its Wikipedia ID
    return requests.get(url + "api/wikipediaId/" + language + "/" + wikipedia_id).json()


def getOEKGIdsByWikipediaIds(language, *wikipedia_ids):
    # Get OEKG IDs for a set of Wikidata IDs
    ids = {}
    for ids_sublist in np.array_split(wikipedia_ids, math.ceil(len(wikipedia_ids) / 5000)):
        res = requests.post(url + "api/wikidataIds/",data=";".join(ids_sublist)).json()
        ids = merge(ids, res)
    return ids


def clear_graph(graph_to_be_cleared):
    # Remove all triples uploaded within the given graph.
    print("Clear graph " + graph_to_be_cleared+".")
    r = requests.get(url + "clear/"+graph_to_be_cleared)
    print(r.text)


def query_oekg(query):
    result1 = requests.get(url + "sparql?query=" + urllib.parse.quote_plus(query) + "&format=json")
    print("RES: "+str(result1.json()))

if __name__ == '__main__':
    insert_example_triples()

    # Run an example query
    query = ("SELECT ?mainEntity ?title WHERE { "
              "?article so:mainEntity ?mainEntity .  "
              "?mainEntity owl:sameAs dbr:Joe_Biden . "
              "?article so:headline ?title . }")
    query_oekg(query)