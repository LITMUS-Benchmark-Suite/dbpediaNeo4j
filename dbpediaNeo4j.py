#!/usr/bin/python

from neo4j import GraphDatabase
from datetime import datetime
import RDF, sys, os, subprocess
from os import listdir
from os.path import isfile, join

import sys, os, signal

from multiprocessing import Process, Queue, active_children, Manager


def checkArgs():
    if len(sys.argv) < 2:
        return False
    else:
        return True


def createDB():
    db = GraphDatabase('dbpedia-graph.db')
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

def makeGraph(db, index, f, counter, i, finished):
	
	file_lines = int((subprocess.check_output(['wc', '-l', f])).split()[0])
        # RDF parses dbpedia ntriples dump
        print "parsing ", f
        startTime = datetime.now()
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
                        sys.stdout.write( "\r%s Progress: %d%%" % (f, perc))
                        sys.stdout.flush()        
	endTime = datetime.now()
	finished[i] = True
	print "\n%s Finished - %d relationships imported in %d seconds" % (f, counter, (endTime - startTime).seconds)


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
    
    dbfiles = [join(sys.argv[1], f) for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f)) and str(join(sys.argv[1], f))[-3:] == '.nt']
    startTime = datetime.now()
    i = 0
    allfinished = []
    for f in dbfiles:
	counter = 0.0
	allfinished.append(False)
	p = Process(target=makeGraph, args=(db,index, f, counter, i, allfinished,))
	p.start()
	i += 1
    while True:
	if False in allfinished:
		continue
	else:
		break
    # Shutdown db
    db.shutdown()
    endTime = datetime.now()
    print "\nAll Finished - relationships imported in %d seconds" % ((endTime - startTime).seconds)
    print "Move %s/dbpedia-graph.db to your Neo4j data directory when this program finishes;-)" % os.getcwd()
    #return


if __name__ == '__main__':
    main()
