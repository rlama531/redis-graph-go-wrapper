package redisgraphgowrapper

import (
	"fmt"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"

	rg "github.com/redislabs/redisgraph-go"
)

type RedisGraphProvider struct {
	graph *rg.Graph
}

func NewRedisConnection(addr string, pass string) redis.Conn {
	conn, _ := redis.Dial("tcp", addr)
	return conn
}

func NewRedisConnectionGraphProvider(conn redis.Conn, graphName string) *RedisGraphProvider {
	graph := rg.GraphNew(graphName, conn)

	return &RedisGraphProvider{
		graph: &graph,
	}
}

// ClearGraph deletes the graph created by the provider.
func (g *RedisGraphProvider) ClearGraph() {
	g.graph.Delete()
}

// AddNode adds a node to the graph. It requires a label to identify the type of node, an optional alias
// string, and a properities map[string]interface{}. In the properties map, "id" key must be present.
func (g *RedisGraphProvider) AddNode(label string, alias string, properties map[string]interface{}) error {
	if _, ok := properties["id"]; !ok {
		return errors.New("property did not contain an id. cannot add node")
	}

	node := rg.NodeNew(label, alias, properties)
	g.graph.AddNode(node)
	if _, err := g.graph.Flush(); err != nil {
		return err
	}
	return nil
}

// GetAllNodes returns a list of all the nodes present in the graph
func (g *RedisGraphProvider) GetAllNodes() ([]*rg.Node, error) {
	query := "MATCH (s) RETURN s"
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Node{}, nil
	}

	nodelist, err := extractNodeListFromQueryResult(result)
	if err != nil {
		return nil, err
	}

	return nodelist, nil
}

// GetNodeByID grabs a node by the id property.
func (g *RedisGraphProvider) GetNodeByID(srcNodeID string) (*rg.Node, error) {
	query := fmt.Sprintf("MATCH (s{id:'%s'}) RETURN s", srcNodeID)
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return nil, fmt.Errorf("could not find node with ID %s", srcNodeID)
	}

	result.Next()
	rec := result.Record()

	s, ok := rec.GetByIndex(0).(*rg.Node)
	if !ok {
		return nil, errors.New("getting node failed")
	}

	return s, nil
}

// GetAllNodesWithLabel grabs all nodes with the matching label string.
func (g *RedisGraphProvider) GetAllNodesWithLabel(label string) ([]*rg.Node, error) {
	return g.GetAllNodesWithLabels([]string{label})
}

// GetAllNodesWithLabels grabs all nodes that match any of the labels string slice.
func (g *RedisGraphProvider) GetAllNodesWithLabels(labels []string) ([]*rg.Node, error) {
	var queryLabels []string
	for _, label := range labels {
		labelFormatted := fmt.Sprintf("s:%s", label)
		queryLabels = append(queryLabels, labelFormatted)
	}

	queryLabel := strings.Join(queryLabels, " OR ")
	query := fmt.Sprintf("MATCH (s) WHERE %s RETURN s", queryLabel)
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Node{}, nil
	}

	nodeList, err := extractNodeListFromQueryResult(result)
	if err != nil {
		return nil, errors.Wrap(err, "could not extract node list")
	}

	return nodeList, nil
}

// GetAllNodesWithProperties will return the nodes that match the properties given. If fullMatch is true, then the func will
// only return nodes that match all the properties given. If false, then it will return all nodes that match at least
// one of the properties given.
func (g *RedisGraphProvider) GetAllNodesWithProperties(properties map[string]interface{}, fullMatch bool) ([]*rg.Node, error) {
	var propertiesWhereClause string
	p := make([]string, 0, len(properties))
	for k, v := range properties {
		p = append(p, fmt.Sprintf("s.%s = %v", k, rg.ToString(v)))
	}
	if fullMatch {
		propertiesWhereClause = strings.Join(p, " AND ")
	} else {
		propertiesWhereClause = strings.Join(p, " OR ")
	}

	query := fmt.Sprintf("MATCH (s) WHERE %s RETURN s", propertiesWhereClause)
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Node{}, nil
	}

	nodeList, err := extractNodeListFromQueryResult(result)
	if err != nil {
		return nil, errors.Wrap(err, "could not extract node list")
	}

	return nodeList, nil
}

// GetNthDegreeNodesByRelationAndExcludeShorterDegree will return all the nthDegree nodes with destNodeProperties that are related to the src node via the relation paramter.
// It will NOT return nodes of the nth degree that also have a shorter degree. For example, if nodeA is connected to nodeB by 1st and 2nd degree,
// using this func with 2 as the nthDegree will NOT return it since it is also a 1st degree connection.
func (g *RedisGraphProvider) GetNthDegreeNodesByRelationAndExcludeShorterDegree(srcNodeID string, relation string, nthDegree int, destNodeProperties map[string]interface{}) ([]*rg.Node, error) {
	nthDegreeList, err := g.getNthDegreeNodesByRelationAndDestNodeProperities(srcNodeID, relation, nthDegree, destNodeProperties)
	if err != nil {
		return nil, err
	} else if nthDegree == 1 {
		return nthDegreeList, nil
	}

	var propertiesString string

	if len(destNodeProperties) > 0 {
		propertiesString = encodeProperitiesToString(destNodeProperties)
	}

	query := fmt.Sprintf(`
		MATCH (s {id: '%s'})-[:%s*1..%d]->(r %s)
		RETURN r
	`, srcNodeID, relation, nthDegree-1, propertiesString)

	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Node{}, nil
	}

	newNodeList, err := extractNodeListFromQueryResult(result)
	if err != nil {
		return nil, errors.Wrap(err, "could not extract node list")
	}

	filteredNodeList := nodeListMinusJoinOperation(nthDegreeList, newNodeList)
	return filteredNodeList, nil
}

// getNthDegreeNodesByRelationAndDestNodeProperities will return all the nthDegree nodes that are related to the src node via the relation paramter.
// It can return nodes that are also related on fewer degrees. For example, if nodeA is connected to nodeB by 1st and 2nd degree,
// using this func with 2 as the nthDegree will return it, even though it is also a 1st degree connection.
func (g *RedisGraphProvider) GetNthDegreeNodesByRelation(srcNodeID string, relation string, nthDegree int, destNodeProperties map[string]interface{}) ([]*rg.Node, error) {
	return g.getNthDegreeNodesByRelationAndDestNodeProperities(srcNodeID, relation, nthDegree, destNodeProperties)
}

// getNthDegreeNodesByRelationAndDestNodeProperities will return all the nthDegree nodes with destNodeProperties that are related to the src node via the relation paramter.
// It can return nodes that are also related on fewer degrees. For example, if nodeA is connected to nodeB by 1st and 2nd degree,
// using this func with 2 as the nthDegree will return it, even though it is also a 1st degree connection.
func (g *RedisGraphProvider) getNthDegreeNodesByRelationAndDestNodeProperities(srcNodeID string, relation string, nthDegree int, destNodeProperties map[string]interface{}) ([]*rg.Node, error) {
	var propertiesString string

	if len(destNodeProperties) > 0 {
		propertiesString = encodeProperitiesToString(destNodeProperties)
	}
	query := fmt.Sprintf(`
		MATCH (s {id: '%s'})-[:%s*%d]->(r %s)
		RETURN r
	`, srcNodeID, relation, nthDegree, propertiesString)

	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Node{}, nil
	}

	nodeList, err := extractNodeListFromQueryResult(result)
	if err != nil {
		return nil, errors.Wrap(err, "could not extract node list")
	}

	return nodeList, nil
}

/* END NODES */

/* START EDGES */

type EdgeDirection int32

const (
	Undirected EdgeDirection = iota
	Directed
)

// AddEdge will add an edge which can directed (from the specified srcNodeID to the destNodeID) or undirected. The srcNodeID and destNodeID params refer to the
// "id" properties on the nodes, the relation param specifies the type of relationship, and the properties map must include "id" for the edge.
func (g *RedisGraphProvider) AddEdge(edgeDirection EdgeDirection, srcNodeID string, destNodeID string, relation string, properties map[string]interface{}) error {
	if _, ok := properties["id"]; !ok {
		return errors.New("properties did not contain an id")
	} else if relation == "" || srcNodeID == "" || destNodeID == "" {
		return errors.New("edge relation is empty")
	}

	// Make sure srcNode exists
	_, err := g.GetNodeByID(srcNodeID)
	if err != nil {
		return err
	}

	// Make sure destNode exists
	_, err = g.GetNodeByID(destNodeID)
	if err != nil {
		return err
	}

	propertyString := encodeProperitiesToString(properties)

	var queryDirection string
	switch edgeDirection {
	case Undirected:
		queryDirection = "-"
	case Directed:
		queryDirection = "->"
	default:
		return errors.New("unrecognized edge direction")
	}
	query := fmt.Sprintf(`
		MATCH 
			(srcNode {id:'%s'}), 
			(destNode {id:'%s'})
		MERGE (srcNode)-[r:%s %s]%s(destNode) 
		RETURN r`, srcNodeID, destNodeID, relation, propertyString, queryDirection)

	if _, err := g.graph.Query(query); err == nil {
		return errors.Wrap(err, "erro creating edge")
	}

	return err
}

// GetAllEdges grabs all edges that exist in the graph .
func (g *RedisGraphProvider) GetAllEdges() ([]*rg.Edge, error) {
	result, err := g.graph.Query("MATCH (s)-[e]->(d) RETURN s,e,d")
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Edge{}, nil
	}

	edgeList := []*rg.Edge{}
	for result.Next() {
		r := result.Record()
		srcNode, ok := r.GetByIndex(0).(*rg.Node)
		if !ok {
			return nil, errors.New("getting source node failed")
		}
		edge, ok := r.GetByIndex(1).(*rg.Edge)
		if !ok {
			return nil, errors.New("getting egde failed")
		}

		destNode, ok := r.GetByIndex(2).(*rg.Node)
		if !ok {
			return nil, errors.New("getting dest node failed")
		}

		edge.Source = srcNode
		edge.Destination = destNode
		edgeList = append(edgeList, edge)
	}

	return edgeList, nil
}

// GetEdgesByRelation grabs all edges that exist in the graph that match the given relation
func (g *RedisGraphProvider) GetEdgesByRelation(relation string) ([]*rg.Edge, error) {
	return g.GetEdgesByRelations([]string{relation})
}

// GetEdgesByRelations grabs all edges that exist in the graph that match the given relations.
func (g *RedisGraphProvider) GetEdgesByRelations(relations []string) ([]*rg.Edge, error) {
	queryLabel := strings.Join(relations, "|")
	query := fmt.Sprintf("MATCH (s)-[e:%s]->(d) RETURN s,e,d", queryLabel)
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, err
	} else if result.Empty() {
		return []*rg.Edge{}, nil
	}

	edgeList := []*rg.Edge{}
	for result.Next() {
		r := result.Record()
		srcNode, ok := r.GetByIndex(0).(*rg.Node)
		if !ok {
			return nil, errors.New("getting source node failed")
		}
		edge, ok := r.GetByIndex(1).(*rg.Edge)
		if !ok {
			return nil, errors.New("getting egde failed")
		}

		destNode, ok := r.GetByIndex(2).(*rg.Node)
		if !ok {
			return nil, errors.New("getting dest node failed")
		}

		edge.Source = srcNode
		edge.Destination = destNode
		edgeList = append(edgeList, edge)
	}

	return edgeList, nil
}

// GetEdgesByRelations grabs all edges that exist in the graph that match the given relations.
func (g *RedisGraphProvider) GetEdgesByNodeIDs(srcNodeID string, destNodeID string, optionalRelation string) ([]*rg.Edge, error) {
	queryLabel := optionalRelation
	if optionalRelation != "" {
		queryLabel = fmt.Sprintf(":%s", optionalRelation)
	}

	query := fmt.Sprintf("MATCH (s {id: '%s'})-[e%s]->(d {id: '%s'}) RETURN s,e,d", srcNodeID, queryLabel, destNodeID)
	result, err := g.graph.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "error on query for get edges by node IDs")
	} else if result.Empty() {
		return []*rg.Edge{}, nil
	}

	edgeList := []*rg.Edge{}
	for result.Next() {
		r := result.Record()
		srcNode, ok := r.GetByIndex(0).(*rg.Node)
		if !ok {
			return nil, errors.New("getting source node failed")
		}
		edge, ok := r.GetByIndex(1).(*rg.Edge)
		if !ok {
			return nil, errors.New("getting egde failed")
		}

		destNode, ok := r.GetByIndex(2).(*rg.Node)
		if !ok {
			return nil, errors.New("getting dest node failed")
		}

		edge.Source = srcNode
		edge.Destination = destNode
		edgeList = append(edgeList, edge)
	}

	return edgeList, nil
}

/* END EDGES */
/* START HELPERS */

// encodeProperitiesToString is a helper function to encode properties map[string]interface{} into
// a queryable form.
func encodeProperitiesToString(properties map[string]interface{}) string {
	p := make([]string, 0, len(properties))
	for k, v := range properties {
		p = append(p, fmt.Sprintf("%s:%v", k, rg.ToString(v)))
	}

	return fmt.Sprintf("{%s}", strings.Join(p, ","))
}

// extractNodeListFromQueryResult takes a redis graph query result and
// parses it to get a list of nodes. The result should be expected to only
// contain nodes and will error out if not.
func extractNodeListFromQueryResult(result *rg.QueryResult) ([]*rg.Node, error) {
	nodeList := []*rg.Node{}
	for result.Next() {
		r := result.Record()
		node, ok := r.GetByIndex(0).(*rg.Node)
		if !ok {
			return nil, errors.New("getting node failed")
		}
		nodeList = append(nodeList, node)
	}
	return nodeList, nil
}

func nodeListMinusJoinOperation(plusList []*rg.Node, minusList []*rg.Node) []*rg.Node {
	var resultList []*rg.Node
	var found bool
	for _, plusNode := range plusList {
		found = false
		for _, minusNode := range minusList {
			if plusNode.GetProperty("id") == minusNode.GetProperty("id") {
				found = true
				break
			}
		}
		if !found {
			resultList = append(resultList, plusNode)
		}
	}
	return resultList
}
