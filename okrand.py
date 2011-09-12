#! /usr/env/bin python

from rdflib import BNode, Namespace, RDF, Literal
from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store, VALID_STORE, CORRUPTED_STORE, NO_STORE, UNKNOWN

from pymarc import MARCReader


DC = Namespace("http://purl.org/dc/elements/1.1/")
DCTERMS = Namespace("http://purl.org/dc/terms/")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
GR = Namespace('http://www.heppnetz.de/ontologies/goodrelations/v1.html#')
PRISM = Namespace('http://prismstandard.org/namespaces/1.2/basic/')
RDF=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS=Namespace("http://www.w3.org/2000/01/rdf-schema#")
OWL=Namespace("http://www.w3.org/2002/07/owl#")
SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')

NLNZCAT = Namespace('http://nlnzcat.natlib.govt.nz/vprimo/getHoldings?bibId=') #NLNZils
WORLDCAT = Namespace('http://www.worldcat.org/search?q=')

# See also
#   http://dcpapers.dublincore.org/ojs/pubs/article/viewFile/916/912
#   http://www.loc.gov/marc/umb/um07to10.html#part7
#   http://www.loc.gov/marc/marc2dc.html

desc = {'*': DCTERMS['description']}

marc_to_rdf= {
    '001' : {'*': DCTERMS['identifier']},
    '003' : {'*': DCTERMS['identifier']},
    '005' : {'*': DCTERMS['modified']},
    '008' : {'*': DCTERMS['created']},
    '010' : {'*': DCTERMS['identifier']},
    '020' : {'a': DCTERMS['identifier'], 'c' : GR['hasCurrencyValue']},
    '022' : {'*': DCTERMS['identifier']}, #ISSN
    '033' : {'a': DC['Coverage']},
    '034' : {'*': DC['Coverage']},
    '035' : {'*': DCTERMS['identifier']},
    #'040' : {}, # cataloging source, e.g. NatLib
    '043' : {'c': DC['Coverage']},
    '044' : {'c': DC['Coverage']},
    '046' : {'j': DCTERMS['modified'], 'm': 'dc:Date Valid'},
    '050' : {'*': DC['LLC']},
    '053' : {'a': DC['LLC']},
    '060' : {'*': DC['NLM']},
    '080' : {'*': DC['UDC']},
    '082' : {'a': DC['DDC']},
    '100' : {'a': DCTERMS['contributor']}, #TODO 'd' is author's lifespan 
    '110' : {'*': DCTERMS['contributor']},
    '111':  {'*': DCTERMS['contributor']},
    '130' : {'a': DC['Alternative'], 'l': DC['Language']},
    '150' : {'a': SKOS['prefLabel']},
    '151' : {'a': SKOS['prefLabel']},
    '210' : {'*': DC['Alternative']},
    '240' : {'a': DC['Alternative'], 'l': DC['Language'], 'f': DC['Date']},
    "245" : {'a': DC['Title'], 'h': DCTERMS['medium']},
    "246" : {'a': DC['Alternative']},
    "247" : {'a': DC['Alternative']},
    "250" : {'a': DCTERMS['hasVersion']},
    '255' : {'*': DC['Coverage']},
    "260" : {'b': DCTERMS['publisher'], 'c': DCTERMS['created']}, #a : place of publication
    "300" : {'a': DC['Extent'], 'c' : GR['height']},
    '310' : {'a': DC['AccuralPeriodicity']},
    '450' : {'*': SKOS['altLabel']},
    '451' : {'*': SKOS['altLabel']},
    '500' : desc,
    '501' : desc,
    '502' : desc,
    '503' : desc,
    '504' : desc,
    '505' : {'*': DC['TableOfContents']},
    '506' : {'a': DC['AccessRights'], 'd':DC['AccessRights']},
    '507' : desc,
    '508' : desc,
    '509' : desc,
    '510' : {'*': DC['IsReferencedBy']},
    '511' : desc,
    '512' : desc,
    '513' : desc,
    '514' : desc,
    '515' : desc,
    '516' : desc,
    '517' : desc,
    '518' : desc,
    '519' : desc,
    '520' : {'a': DCTERMS['abstract'], 'b':DCTERMS['abstract']},
    '521' : {'*': DC['Audience']},
    '522' : {'*': DC['Coverage']},
    '523' : desc,
    '524' : desc,
    '525' : desc,
    '526' : desc,
    '527' : desc,
    '528' : desc,
    '529' : desc,
    '530' : {'*': DC['hasFormat']},
    '533' : {'b': DC['Coverage'], 'e': DC['Extent']},
    '534' : {'t': DCTERMS['source']},
    '535' : desc,
    '536' : desc,
    '537' : desc,
    '538' : {'*': DC['Requires']},
    '539' : desc,
    '540' : {'*': DCTERMS['rights']},
    '541' : {'c': DC['AccuralMethod']},
    '542' : {'d': DC['RightsHolder'], 'g': DC['DateCopyrighted']},
    '543' : desc,
    '544' : desc,
    '545' : desc,
    '546' : {'*': DC['Language']},
    '547' : desc,
    '548' : desc,
    '549' : desc,
    '550' : desc,
    '551' : desc,
    '552' : desc,
    '553' : desc,
    '554' : desc,
    '555' : desc,
    '556' : desc,
    '557' : desc,
    '558' : desc,
    '559' : desc,
    '560' : desc,
    '561' : {'*': DC['Provenance']},
    '562' : desc,
    '563' : desc,
    '564' : desc,
    '565' : desc,
    '566' : desc,
    '567' : desc,
    '568' : desc,
    '569' : desc,
    '570' : desc,
    '571' : desc,
    '572' : desc,
    '573' : desc,
    '574' : desc,
    '575' : desc,
    '576' : desc,
    '577' : desc,
    '578' : desc,
    '579' : desc,
    '580' : desc,
    '581' : desc,
    '582' : desc,
    '583' : desc,
    '584' : desc,
    '585' : desc,
    '586' : desc,
    '587' : desc,
    '588' : desc,
    '589' : desc,
    '590' : desc,
    '591' : desc,
    '592' : desc,
    '593' : desc,
    '594' : desc,
    '595' : desc,
    '596' : desc,
    '597' : desc,
    '598' : desc,
    '599' : desc,
    '600' : {'*': DCTERMS['subject']}, #person
    '610' : {'*': DCTERMS['subject']},
    '611' : {'*': DCTERMS['subject']},
    '630' : {'*': DCTERMS['subject']},
    '650' : {'*': DCTERMS['subject']}, #topic FIXME: 'z' is geo coverage 
    "651" : {'*': DCTERMS['spatial']}, #geography
    '653' : {'*': DCTERMS['subject']},
    '655' : {'*': DC['Type']},
    "662" : {'*': DCTERMS['coverage']},
    "667" : {'*': SKOS['note']},
    '670' : {'*': DCTERMS['source']},
    '675' : {'*': SKOS['editorialNote']},
    '678' : {'*': SKOS['definition']},
    '680' : {'*': SKOS['scopeNote']},
    '681' : {'*': SKOS['historyNote']},
    '682' : {'*': SKOS['changeNote']},
    '688' : {'*': SKOS['historyNote']},
    '700' : {'*': DCTERMS['contributor']},
    '710' : {'*': DCTERMS['contributor']},
    '711' : {'*': DCTERMS['contributor']},
    '720' : {'*': DCTERMS['contributor']},
    '740' : {'a': DCTERMS['alternative']},
    '760' : {'*': DC['IsPartOf']},
    '773' : {'*': DC['IsPartOf']},
    '774' : {'*': DC['HasPart']},
    '775' : {'*': DC['HasVersion']},
    '785' : {'n': DC['IsReferencedBy'], 't': DC['IsReferencedBy'], 'o': DC['IsReferencedBy']},
    '776' : {'n': DC['hasFormat'], 't': DC['hasFormat']},
    '786' : {'o': DCTERMS['source'], 't': DCTERMS['source'], 'n':DC['IsVersionOf'], 'o': DC['IsVersionOf']},
    '780' : {'n': DC['Replaces'], 't': DC['Replaces'], 'o': DC['Replaces']},
    '800' : {'*': DC['IsPartOf']},
    '810' : {'*': DC['IsPartOf']},
    '811' : {'*': DC['IsPartOf']},
    '830' : {'*': DC['IsPartOf']},
    '856' : {'u': DC['URI']}
}

def process_leader(record):
    """
    http://www.loc.gov/marc/marc2dc.html#ldr06conversionrules
    """
    broad_06 = dict(
        a="Text",
        c="Text",
        d="Text",
        e="Image",
        f="Image",
        g="Image",
        i="Sound",
        j="Sound",
        k="Image",
        m="Software",
        p="Collection",
        t="Text")
    
    detailed_06 = dict(
        a="language material",
        c="printed music",
        d="manuscript music",
        e="cartographic material",
        f="manuscript cartographic material",
        g="projected medium",
        i="nonmusical sound recording",
        j="musical sound recording",
        k="2-dimensional nonprojectable graphic",
        m="computer file",
        o="kit",
        p="mixed materials",
        r="3-dimensional artifact or naturally occurring object",
        t="manuscript language material")
    
    _06 = record.leader[6]
    if _06 in broad_06.keys():
        yield (DC['Type'], broad_06[_06])
    if _06 in detailed_06.keys():
        yield (DC['Type'], detailed_06[_06])
    if record.leader[7] in ('c', 's'):
        yield (DC['Type'], 'Collection')

def process_008(record):
    """
    http://www.loc.gov/marc/umb/um07to10.html#part9
    """
    audiences = {
        'a':'preschool',
        'b':'primary',
        'c':'pre-adolescent',
        'd':'adolescent',
        'e':'adult',
        'f':'specialized',
        'g':'general',
        'j':'juvenile'}

    media = {
        'a':'microfilm',
        'b':'microfiche',
        'c':'microopaque',
        'd':'large print',
        'f':'braille',
        'r':'regular print reproduction',
        's':'electronic'
        }

    types = {
        "a":"abstracts/summaries",
        "b":"bibliographies (is one or contains one)",
        "c":"catalogs",
        "d":"dictionaries",
        "e":"encyclopedias",
        "f":"handbooks",
        "g":"legal articles",
        "i":"indexes",
        "j":"patent document",
        "k":"discographies",
        "l":"legislation",
        "m":"theses",
        "n":"surveys of literature",
        "o":"reviews",
        "p":"programmed texts",
        "q":"filmographies",
        "r":"directories",
        "s":"statistics",
        "t":"technical reports",
        "u":"standards/specifications",
        "v":"legal cases and notes",
        "w":"law reports and digests",
        "z":"treaties"}
    
    govt_publication = {
        "i":"international or intergovernmental publication",
        "f":"federal/national government publication",
        "a":"publication of autonomous or semi-autonomous component of government",
        "s":"government publication of a state, province, territory, dependency, etc.",
        "m":"multistate government publication",
        "c": "publication from multiple local governments",
        "l": "local government publication",
        "z":"other type of government publication",
        "o":"government publication -- level undetermined",
        "u":"unknown if item is government publication"}

    genres = {
	"0":"not fiction",
        "1":"fiction",
        "c":"comic strips",
        "d":"dramas",
        "e":"essays",
        "f":"novels",
        "h":"humor, satires, etc.",
        "i":"letters",
        "j":"short stories",
        "m":"mixed forms",
        "p":"poetry",
        "s":"speeches"}

    biographical = dict(
        a="autobiography",
        b='individual biography',
        c='collective biography',
        d='contains biographical information')
    
    info = record['008'].value()
    yield (DCTERMS['date'], '{}-{}-{}'.format(info[0:2], info[2:4], info[4:6]))
    for i, field in enumerate(info):
        try:
            if i < 23 or field in ('#',  ' ', '|'):
                continue
            elif i == 23:
                yield (DCTERMS['medium'], media[info[23]])
            elif i >= 24 and i <= 27:
                yield (DC['Type'], types[info[i]])
            elif i == 28:
                yield (DC['Type'], govt_publication[info[28]])
            elif i == 29 and field == '1':
                yield (DC['Type'], 'conference publication')
            elif i == 30 and field == '1':
                yield (DC['Type'], 'festschrift')
            elif i == 33:
                if field != 'u': #unknown
                        yield (DC['Type'], genres[info[33]])
            elif i == 34:
                try:
                    yield (DC['Type'], biographical[info[34]])
                except KeyError :
                    # logging.warn('something')
                    pass
            else:
                continue
        except KeyError:
            # ':('
            pass

#TODO languages

def process_records(books):
    for book in books:
        for triple in process_record(book):
            yield triple

def process_record(book):
    node = None
    for i, tag in enumerate([field.tag for field in book.fields]):
        if tag == '035':
            val = book.fields[i].value()
            if 'NLNZils' in val:
                node = NLNZCAT[val.split(')')[1]]
                break
            elif 'OCoLC' in val:
                node = WORLDCAT[val.split(')')[1]]
                break
    if node is None:
            node = BNode()
    for find in process_leader(book):
        yield (node, find[0], Literal(find[1]))
    for find in process_008(book):
        yield (node, find[0], Literal(find[1]))
    for field in book.fields:
        if field.is_control_field():
            continue
        elif field.tag == '035':
            val = field.value()
            if 'NLNZils' in val:
                    yield (node, OWL['sameAs'], NLNZCAT[val.split(')')[1]])
            elif 'OCoLC' in val:
                    yield (node, OWL['sameAs'], WORLDCAT[val.split(')')[1]])
            yield (node, DC['Identifier'], Literal(field.value()))
        else:
            subfields = {}
            for idx, val in enumerate(field.subfields):
                    if idx % 2 == 0:
                            subfields[val] = field.subfields[idx+1]
            try:
                rule = marc_to_rdf[field.tag]
            except KeyError:
                continue
            if '*' in rule:
                try:
                    yield (node, rule['*'], Literal(' '.join(subfields.values())))
                except TypeError:
                    print 'TypeError: ', field.tag, subfields
                except UnicodeDecodeError:
                    print 'UnicodeDecodeError:', field.tag, subfields
            else:
                for k in subfields.keys():
                    try:
                        yield (node, rule[k], Literal(subfields[k]))
                    except KeyError: continue


def main(fd, store_type=None, store_id=None, graph_id=None, gzipped=False):
    """
    Converts MARC21 data stored in fd to a RDFlib graph.
    """
    from rdflib import plugin

    if store_type:
        msg = "Need a {} identifier for a disk-based store."
        assert store_id, msg.format('store')
        assert graph_id, msg.format('graph')
        store = plugin.get(store_type, Store)(store_id)
    else:
        store = 'default'

    graph = Graph(store=store, identifier=graph_id)

    if gzipped:
        import gzip
        open = gzip.open

    try:
        records = MARCReader(open(fd))

        for i, triple in enumerate(process_records(records)):
            graph.add(triple)
            if i % 100 == 0:
                graph.commit()
            if i % 10000 == 0:
                print i
    finally:
        records.close()

    return graph
