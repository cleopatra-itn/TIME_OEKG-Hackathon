import os
import json
import pandas as pd
import logging

from helper_functions import *

## My Graph
graph = "time"

## Clear/Reset Graph
clear_graph(graph)

uploadFileToOEKG(graph, "oekg_extension/graphs/euroscepticism.nt")
uploadFileToOEKG(graph, "oekg_extension/graphs/time_articles.nt")
