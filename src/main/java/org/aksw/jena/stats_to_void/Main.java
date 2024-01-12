package org.aksw.jena.stats_to_void;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.engine.optimizer.StatsMatcher;
import org.apache.jena.sparql.sse.Item;
import org.apache.jena.sparql.sse.ItemList;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jena.sparql.engine.optimizer.reorder.PatternElements.*;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static long count;
    public static long classes;
    public static Model m = ModelFactory.createDefaultModel();
    public static Resource ds;

    public static void main(String[] args) {
        if (args.length >= 3 && !args[2].isBlank()) {
            ds = m.createResource(args[2]);
        } else {
            ds = m.createResource();
        }
        if (args.length < 1) {
            System.err.println("Syntax: stats-to-void.jar stats.opt [<sparql endpoint> [<dataset IRI>]] > stats.ttl");
            System.exit(1);
        }
        String filename = args[0];
        Item stats = SSE.readFile(filename);
        if ( stats.isNil() ) {
            log.warn("Empty stats file: " + filename);
            return;
        }
        if ( !stats.isTagged(StatsMatcher.STATS) )
            throw new ARQException("Not a stats file: " + filename);
        init(stats);
        m.withDefaultMappings(PrefixMapping.Standard);
        m.setNsPrefix("void", VOID.NS);
        m.setNsPrefix("dcterms", DCTerms.NS);
        if (!m.isEmpty()) {
            ds.addProperty(RDF.type, VOID.Dataset);
            ds.addProperty(VOID.classes, m.createTypedLiteral(classes));
            if (args.length >= 2 && !args[1].isBlank()) {
                ds.addProperty(VOID.sparqlEndpoint, m.createResource(args[1]));
            }
        }
        m.write(System.out, "TURTLE");
    }

    protected static void init(Item stats) {
        if ( !stats.isTagged(StatsMatcher.STATS) )
            throw new ARQException("Not a tagged '" + StatsMatcher.STATS + "'");

        ItemList list = stats.getList().cdr();      // Skip tag

        if ( list.car().isTagged(StatsMatcher.META) ) {
            // Process the meta tag.
            Item elt1 = list.car();
            list = list.cdr();      // Move list on

            // Get count.
            Item x = Item.find(elt1.getList(), StatsMatcher.COUNT);
            if ( x != null ) {
                count = x.getList().get(1).asLong();
                ds.addProperty(VOID.triples, m.createTypedLiteral(count));
            }
            Item timestamp = Item.find(elt1.getList(), "timestamp");
            if ( timestamp != null ) {
                ds.addProperty(DCTerms.date, m.asRDFNode(timestamp.getList().get(1).getNode()));
            }
        }

        while (!list.isEmpty()) {
            Item elt = list.car();
            list = list.cdr();
            onePattern(elt);
        }

    }

    protected static void onePattern(Item elt) {
        Item pat = elt.getList().get(0);

        if ( pat.isNode() ) {
            // (<uri> weight)
            Node n = pat.getNode();
            if ( !n.isURI() ) {
                log.warn("Not a predicate URI: " + pat.toString());
                return;
            }
            addAbbreviation(elt);
        } else if ( pat.isSymbol() ) {
            if ( pat.equals(StatsMatcher.OTHER) ) {
                double d = elt.getList().get(1).getDouble();
                //DefaultMatch = d;
                return;
            }

            if ( pat.equals(BNODE) || pat.equals(LITERAL) ) {
                log.warn("Not a match for a predicate URI: " + pat.toString());
                return;
            }
            if ( pat.equals(TERM) || pat.equals(VAR) || pat.equals(ANY) )
                addAbbreviation(elt);
            else {
                log.warn("Not understood: " + pat);
                return;
            }
        } else if ( pat.isList() && pat.getList().size() == 3 ) {
            // It's of the form ((S P O) weight)
            Item w = elt.getList().get(1);
            addPattern(w.getLong(),
                    intern(pat.getList().get(0)),
                    intern(pat.getList().get(1)),
                    intern(pat.getList().get(2)));
        } else {
            log.warn("Unrecognized pattern: " + pat);
        }

    }

    protected static Item intern(Item item) {
        if ( item.sameSymbol(ANY.getSymbol()) )
            return ANY;
        if ( item.sameSymbol(VAR.getSymbol()) )
            return VAR;
        if ( item.sameSymbol(TERM.getSymbol()) )
            return TERM;
        if ( item.sameSymbol(URI.getSymbol()) )
            return URI;
        if ( item.sameSymbol(LITERAL.getSymbol()) )
            return LITERAL;
        if ( item.sameSymbol(BNODE.getSymbol()) )
            return BNODE;
        return item;
    }

    protected static void addAbbreviation(Item elt) {
        Item predicateTerm = elt.getList().get(0);
        // Single node - it's a predicate abbreviate.
        long numProp = elt.getList().get(1).getLong();

        Resource r = m.createResource();
        r.addProperty(VOID.property, m.asRDFNode(predicateTerm.getNode()));
        r.addProperty(VOID.triples, m.createTypedLiteral(numProp));
        ds.addProperty(VOID.propertyPartition, r);
    }

    private static void addPattern(long count, Item s, Item p, Item o) {
        Node predNode = p.getNode();
        if (predNode.hasURI(RDF.type.getURI())
                || predNode.hasURI("http://www.wikidata.org/prop/direct/P31")) {

            Resource r = m.createResource();
            r.addProperty(VOID._class, m.asRDFNode(o.getNode()));
            r.addProperty(VOID.distinctSubjects, m.createTypedLiteral(count));
            ds.addProperty(VOID.classPartition, r);
            classes++;
        }
    }

}