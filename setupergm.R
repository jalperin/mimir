library(ergm)
library(network)

if (!exists('YEAR'))
  YEAR = '2005'
if (!exists('CITATION_WINDOW'))
  CITATION_WINDOW = '5'
if (!exists('CODE'))
  CODE = 'SS'

nodes = read.csv(paste('data/nodes', YEAR, '_', CODE, '_numeric.csv', sep=''))

edges=read.csv(paste('data/G', YEAR, '_', CITATION_WINDOW, '_', CODE, '_numeric.edgelist', sep=''), sep=' ', header=FALSE)

# drop the weights that are not being read properly 
edges$V4 = NULL
edges$V3 = NULL

net = network(edges, directed=TRUE)

net %v% "missing" = nodes$missing
net %v% "region" = as.character(nodes$region)
net %v% "country" = as.character(nodes$iso)
net %v% "income_level" = as.character(nodes$income_level)

maxNumDyads=network.size(net)^2
control=control.ergm(MPLE.max.dyad.types=maxNumDyads, MCMC.burnin=1000000, MCMC.samplesize=300000, parallel=7)