# the overlay graph problem
In this document I've compiled all relevant information about vectorlink in order to talk about the overlay graph problem.

The overlay graph problem is this:
How do we create a good overlay graph to augment an NSW (navigable small world) graph with long-distance links?

This document will first briefly explain NSW, what we use it for, and what operations can be applied to it. It'll then go into the limitations and explain the need for an overlay graph.

## NSW and relevant operators
We have the following graph data structure `G: (V, d, N)`, where
- `V`: a set of nodes
- `d`: a distance function, which given two nodes calculates a nonnegative real number.
- `N`: a set of neighborhoods, which are `(V, [V;NSIZE])` tuples, listing `NSIZE` (approximate) closest neighbors for each node.

We have a graph constructor like so:
`C: V -> d -> N`

In other words, given a set of nodes and a distance function over those nodes, generate a set of neighborhoods.

We would like one neighborhood for each node `v` in V, which contains the `NSIZE` closest elements in `V` for that node (excluding itself), sorted by distance. Doing this perfectly however is prohibitively expensive. Instead, any practical implementation of `C` will use some sort of approximate nearest neighbor algorithm.

For this problem, we do not have to worry too much about the implementation details of `C`, and we can just assume it produces a graph where it is statistically likely that close elements in `V` are connected.

### Application
The purpose of this graph is to support two operations, namely search and nearest-neighbor.

#### Search
`G->V->->[..initial search queue..]->count->[V;count]`

In other words, given a graph, a query vector, an initial search queue, and a desired amount of results, produce the `count` closest matches to this query vector according to the graph's distance function.

This is implemented by traversing the neighborhoods. The pseudocode is roughly as follows
```
search_queue = initial_search_queue; // this is either coming in from a search in a higher layer (supernodes), or it is initialized to a single initial node
while ..search queue changed since last iteration.. // keep going until we hit a fixpoint
  node = ..closest unvisited node in the queue..
  neighbors = neighbors_for(node)
  merged = merge_by_distance(search_queue, neighbors, query_vector, d)
  search_queue = truncate(merged, MAX_QUEUE_LEN)

return search_queue
```

Here, `neighbors_for` picks the appropriate neighbors list from `N`.

`merge_by_distance` is a function that calculates the distance from the query vector for each element in two input lists, and then produces a merged list of results, sorted by that distance.

`truncate` truncates the list to a maximum queue length.

In english, this will keep around a list of match candidates, and improve on those candidates by merging in the neighborhoods of these candidates. Each iteration should either get us better matches, or do nothing, at which point we can return the candidates.

#### Nearest Neighbors
Given a well-constructed graph, a set of nearest neighbors for a node can easily be extracted by taking the list of neighbors, and repeatedly merging in its neighbors.

### Optimization
A graph can be improved for a particular vector using the following algorithm:
```
ideal_neighbors = search(self)
for n in ideal_neighbors:
  if ..self is a better candidate in n's neighborhood:
    ..insert self into neighborhood of n, evicting a more distant entry
```

The entire graph can be improved in this way by just looping over all nodes and performing this operation.

The way we actually generate graph is by creating a best-effort graph as a first pass, then iteratively optimizing that until no more significant improvements are made.

### Hierarchical NSW
As described above, search takes in an initial list of nodes to initiate the search with. For a small well-connected graph any random selection (or even a static selection, like the first node in the graph) will do, but beyond a certain size this is no longer possible, because the number of hops (neighborhood traversals) becomes troo large.

HNSW (Hierarchical Navigable Small World) aims to solve this by introducing supernode graphs. We take a random selection of nodes from our graph that is an order of magnitude smaller, then generate a new NSW with just those nodes. This process can be repeated until we end up with a top-level graph that is easily searchable.

Search is then implemented by first searching in the top layer, using the search result to initiate the search in the layer below, and so on until the bottom layer is reached.

Which nodes to select as supernodes is a bit of an open question. Right now, we're just doing a random selection, but we're also experimenting with promotions and demotions based on measured connectivity.

## The problem
Our graphs are having recall issues. No matter how much we optimize (or maybe because of how we optimize) we end up with local minima, very tight neighborhoods of closely connected things, which then do not connect to anything a bit further out.

The recall issues compound. In an HNSW with several layers, unreachability on one layer will propagate to the layers below. While a perfect recall in an approximate data structure is not necessarily achievable, we would like an approach that could at least get us closer, if we were willing to just throw more computational resources at the optimization. Ideally of course, we converge quickly to a good solution.

## A potential solution: an overlay graph
The way to get out of local minima is to establish additional graph connections that lead out of the local minimal group. Different approaches can be imagined.

### Ideal links
There probably exists an algorithm that generates an ideal overlay graph for a particular graph. Unfortunately, no such algorithm is known to us, not even an approximate version.

### Random links
For each node, we can generate an additional number of virtual connections by randomly selecting nodes to connect to. Given a static seed to the pseudo-random number generator (for example, a combination of layer id, node id and a salt), we could always generate the same number of random connections, avoiding the need of actually having to maintain this additional graph.

### Circulant graphs
Given a list of generator numbers `C`, for each `c in C`, we can imagine that each node `v` (here considered to be a nonnegative integer adressing an element in `V`) is additionally connected to `v+c` and `v-c` (mod |V|).

The choice of `C` is an interesting parameter. Right now we're just using the lowest 12 primes.

## Early outcomes
We've tested our graphs with both additional random links, and with an overlay generated from circulant graphs, and found that both improve our recall and convergence rate, but cirulant graphs were outperforming random links.

Since our nodes are uncorrelated, a circulant graph overlay should effectively also establishes random links, but distributed in a far more grid-like fashion.

It is interesting that this works at all. The extra links aren't likely to be 'good', meaning, they won't be close links. They're also not followed often, as the search algorithm will first consider all 'proper', low distance neighbors, which in many cases will lead to evictions of a lot of these extra links. Nevertheless, this appears to work.
