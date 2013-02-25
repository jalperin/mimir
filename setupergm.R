setwd('~/Code/git/mimir')

library(network)
library(ergm)
# library(ergm.userterms)

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

# this is stupid, but we have to read it again for the edge values
edges=read.csv(paste('data/G', YEAR, '_', CITATION_WINDOW, '_', CODE, '_numeric.edgelist', sep=''), sep=' ', header=FALSE)
edges$V3 = NULL
edges$V4 = as.numeric(gsub("}", "", edges$V4))

net %v% "missing" = nodes$missing
net %v% "region" = as.character(nodes$region)
net %v% "country" = as.character(nodes$iso)
net %v% "income_level" = as.character(nodes$income_level)
set.edge.attribute(net, "weight", edges$V4) 

maxNumDyads=network.size(net)^2
control=control.ergm(MPLE.max.dyad.types=maxNumDyads, MCMC.burnin=300000, MCMC.samplesize=100000)

#control=control.ergm(MPLE.max.dyad.types=maxNumDyads, MCMC.burnin=300000, MCMC.samplesize=100000, parallel=7)

# pkgs = names(sessionInfo()$otherPkgs)
# pkgs = paste('package:', pkgs, sep = "")
# lapply(pkgs, detach, character.only = TRUE, unload = TRUE)