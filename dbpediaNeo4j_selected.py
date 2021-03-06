#!/usr/bin/python

from neo4j import GraphDatabase
from datetime import datetime
import RDF, sys, os, subprocess
from os import listdir
from os.path import isfile, join

def checkArgs():
    if len(sys.argv) < 2:
        return False
    else:
        return True


def createDB():
    db = GraphDatabase('dbpedia-small-graph.db')
    if db.node.indexes.exists('dbpedia') == 0:
        index = db.node.indexes.create('dbpedia')
    else:
        index = db.node.indexes.get('dbpedia')
    return db,index


def getFromIndex(index, node):
    return index['name'][node].single

def addToIndex(index, name, node):
    index['name'][name] = node


def createNodes(db, index, a, b, r):
    with db.transaction:
        firstNode = getFromIndex(index, a)
        if firstNode is None:
            firstNode = db.node(name=a)
            addToIndex(index, a, firstNode)
        secondNode = getFromIndex(index, b)
        if secondNode is None:
            secondNode = db.node(name=b)
            addToIndex(index, b, secondNode)
        firstNode.relationships.create(r, secondNode)
        

def main():
    reload(sys)
    sys.setdefaultencoding("UTF8")
    success = checkArgs()
    if not success:
        print "Usage: python dbpediaNeo4j.py /full/path/dirofntfiles"
        sys.exit(1)
    
    # create dbpedia-graph.db
    db,index = createDB()
    counter = 0.0
    dbfiles = ["/srv/Harsh/db_nt/nt_files/labels_en.nt",
    "/srv/Harsh/db_nt/nt_files/article_categories_en.nt",
    "/srv/Harsh/db_nt/nt_files/category_labels_en.nt",
    "/srv/Harsh/db_nt/nt_files/instance_types_en.nt",
    "/srv/Harsh/db_nt/nt_files/instance_types_heuristic_en.nt",
    "/srv/Harsh/db_nt/nt_files/mappingbased_properties_cleaned_en.nt",
    "/srv/Harsh/db_nt/nt_files/page_ids_en.nt",
    "/srv/Harsh/db_nt/nt_files/persondata_en.nt",
    "/srv/Harsh/db_nt/nt_files/raw_infobox_properties_en.nt",
    "/srv/Harsh/db_nt/nt_files/raw_infobox_property_definitions_en.nt",
    "/srv/Harsh/db_nt/nt_files/skos_categories_en.nt",
    "/srv/Harsh/db_nt/nt_files/specific_mappingbased_properties_en.nt",]
    #dbfiles = [join(sys.argv[1], f) for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f)) and join(sys.argv[1], f)[:-3] == '.nt",' ]
    startTime = datetime.now()
    for f in dbfiles:
	file_lines = int((subprocess.check_output(['wc', '-l', f])).split()[0])	
	# RDF parses dbpedia ntriples dump
    	parser = RDF.Parser("ntriples")
    	stream = parser.parse_as_stream("file://" + f)
    	print
    	#startTime = datetime.now()
    	# start parsing
    	for triple in stream:
        	# extract nodes and relationship
        	a = str(triple.subject).split('/')[-1]
        	r = str(triple.predicate).split('/')[-1]
        	b = str(triple.object).split('/')[-1]
        	createNodes(db, index, a, b, r)
        	counter+=1
		# print updated percentage
        	if (counter % 100) == 0:
            		perc = (counter/file_lines)*100
            		sys.stdout.write("\rProgress: %d%%" %perc)
            		sys.stdout.flush()

    # Shutdown db
    db.shutdown()
    endTime = datetime.now()
    print "\nFinished - %d relationships imported in %d seconds" % (counter, (endTime - startTime).seconds)
    print "Move %s/dbpedia-graph.db to your Neo4j data directory ;-)" % os.getcwd()
    return


if __name__ == '__main__':
    main()