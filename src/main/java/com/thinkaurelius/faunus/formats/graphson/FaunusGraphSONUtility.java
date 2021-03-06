package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.*;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FaunusGraphSONUtility {

    private static final String _OUT_E = "_outE";
    private static final String _IN_E = "_inE";
    private static final String EMPTY_STRING = "";

    private static final Set<String> VERTEX_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokens._TYPE, _OUT_E, _IN_E));
    private static final Set<String> EDGE_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokens._TYPE, GraphSONTokens._OUT_V, GraphSONTokens._IN_V));

    private static final FaunusElementFactory elementFactory = new FaunusElementFactory();

    private static final GraphSONUtility graphsonCompact = new GraphSONUtility(GraphSONMode.COMPACT, elementFactory,
            ElementPropertyConfig.ExcludeProperties(VERTEX_IGNORE, EDGE_IGNORE));
    private static final GraphSONUtility graphsonNormal = new GraphSONUtility(GraphSONMode.NORMAL, elementFactory,
            ElementPropertyConfig.ExcludeProperties(VERTEX_IGNORE, EDGE_IGNORE));
    private static final GraphSONUtility graphsonExtended = new GraphSONUtility(GraphSONMode.EXTENDED, elementFactory,
            ElementPropertyConfig.ExcludeProperties(VERTEX_IGNORE, EDGE_IGNORE));

    public static List<FaunusVertex> fromJSON(final InputStream in) throws IOException {
        final List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(FaunusGraphSONUtility.fromJSON(line));
        }
        bfs.close();
        return vertices;

    }

    public static FaunusVertex fromJSON(String line) throws IOException {
        return fromJSON(line, GraphSONMode.COMPACT);
    }

    private static GraphSONUtility getGraphSON(GraphSONMode mode){
        switch(mode){
            case EXTENDED:
                return graphsonExtended;
            case NORMAL:
                return graphsonNormal;
            default:
                return graphsonCompact;
        }

    }

    public static FaunusVertex fromJSON(String line, GraphSONMode mode) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));
            line = EMPTY_STRING; // clear up some memory

            JSONArray outEArray = json.optJSONArray(_OUT_E);
            JSONArray inEArray = json.optJSONArray(_IN_E);

            json.remove(_OUT_E); // clear up some memory
            json.remove(_IN_E); // clear up some memory

            FaunusVertex vertex = (FaunusVertex) getGraphSON(mode).vertexFromJson(json);

            fromJSONEdges(vertex, outEArray, OUT, mode);
            fromJSONEdges(vertex, inEArray, IN, mode);


            return vertex;
        } catch (NullPointerException e){
            return null;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private static void fromJSONEdges(final FaunusVertex vertex, final JSONArray edges, final Direction direction) throws JSONException, IOException {
        fromJSONEdges(vertex, edges, direction, GraphSONMode.COMPACT);
    }

    private static void fromJSONEdges(final FaunusVertex vertex, final JSONArray edges, final Direction direction, GraphSONMode mode) throws JSONException, IOException {
        if (null != edges) {
            for (int i = 0; i < edges.length(); i++) {
                final JSONObject edge = edges.optJSONObject(i);
                FaunusEdge faunusEdge = null;

                if (direction.equals(Direction.IN)) {
                    faunusEdge = (FaunusEdge) getGraphSON(mode).edgeFromJson(edge, new FaunusVertex(edge.optLong(GraphSONTokens._OUT_V)), vertex);
                } else if (direction.equals(Direction.OUT)) {
                    faunusEdge = (FaunusEdge) getGraphSON(mode).edgeFromJson(edge, vertex, new FaunusVertex(edge.optLong(GraphSONTokens._IN_V)));
                }

                if (faunusEdge != null) {
                    vertex.addEdge(direction, faunusEdge);
                }
            }
        }
    }

    public static JSONObject toJSON(final Vertex vertex) throws IOException {
        return toJSON(vertex, GraphSONMode.COMPACT);
    }

    public static JSONObject toJSON(final Vertex vertex, GraphSONMode mode) throws IOException {
        try {
            final JSONObject object = GraphSONUtility.jsonFromElement(vertex, getElementPropertyKeys(vertex, false), mode);

            // force the ID to long.  with blueprints, most implementations will send back a long, but
            // some like TinkerGraph will return a string.  the same is done for edges below
            object.put(GraphSONTokens._ID, Long.valueOf(object.remove(GraphSONTokens._ID).toString()));

            List<Edge> edges = (List<Edge>) vertex.getEdges(OUT);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    final JSONObject edgeObject = GraphSONUtility.jsonFromElement(outEdge, getElementPropertyKeys(outEdge, true), mode);
                    outEdgesArray.put(edgeObject);
                }
                object.put(_OUT_E, outEdgesArray);
            }

            edges = (List<Edge>) vertex.getEdges(IN);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    final JSONObject edgeObject = GraphSONUtility.jsonFromElement(inEdge, getElementPropertyKeys(inEdge, false), mode);
                    inEdgesArray.put(edgeObject);
                }
                object.put(_IN_E, inEdgesArray);
            }

            return object;
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private static Set<String> getElementPropertyKeys(final Element element, final boolean edgeIn) {
        final Set<String> elementPropertyKeys = new HashSet<String>(element.getPropertyKeys());
        elementPropertyKeys.add(GraphSONTokens._ID);
        if (element instanceof Edge) {
            if (edgeIn) {
                elementPropertyKeys.add(GraphSONTokens._IN_V);
            } else {
                elementPropertyKeys.add(GraphSONTokens._OUT_V);
            }

            elementPropertyKeys.add(GraphSONTokens._LABEL);
        }

        return elementPropertyKeys;
    }

    private static class FaunusElementFactory implements ElementFactory<FaunusVertex, FaunusEdge> {
        @Override
        public FaunusEdge createEdge(final Object id, final FaunusVertex out, final FaunusVertex in, final String label) {
            return new FaunusEdge(convertIdentifier(id), out.getIdAsLong(), in.getIdAsLong(), label);
        }

        @Override
        public FaunusVertex createVertex(final Object id) {
            return new FaunusVertex(convertIdentifier(id));
        }

        private long convertIdentifier(final Object id) {
            if (id instanceof Long)
                return (Long) id;

            long identifier = -1l;
            if (id != null) {
                try {
                    identifier = Long.parseLong(id.toString());
                } catch (final NumberFormatException e) {
                    identifier = -1l;
                }
            }
            return identifier;
        }
    }
}