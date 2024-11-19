# HetIONet Analysis with PySpark
This project analyzes the HetIONet network using PySpark to uncover relationships between drugs, genes, and diseases. The analysis is divided into three key questions, each providing insights into drug-gene and drug-disease associations.
## Prerequisites
To run this project, ensure the following dependencies are installed:
- Python (>= 3.8) 
- PySpark
- argparse
## Dataset
Place the following files in the specified paths:
1. **Nodes File:** `C:/Users/natha/Project 2/nodes.tsv`
This file contains the metadata for nodes in the HetIONet graph.
2. **Edges File:** `C:/Users/natha/Project 2/edges.tsv`
This file contains the relationships (edges) between nodes, including the `metaedge` column for relationship types.
Ensure the files are tab-separated and have headers.
 ## Usage
Run the script with the appropriate question flag:
```bash
python hetionet_analysis.py --question <question_number>


Ex: python hetionet_analysis.py --question question1
