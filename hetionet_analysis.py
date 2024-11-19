import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc

# Initialize a SparkSession
spark = SparkSession.builder.appName("HetIONetAnalysis").getOrCreate()
 
# Correct file paths
nodes_path = "C:/Users/natha/Project 2/nodes.tsv"
edges_path = "C:/Users/natha/Project 2/edges.tsv"

def question1():
    # Load nodes and edges data
    nodes_df = spark.read.option("delimiter", "\t").option("header", True).csv(nodes_path)
    edges_df = spark.read.option("delimiter", "\t").option("header", True).csv(edges_path)
    
    # Filter edges for drug-gene (CuG, CdG, CbG) and drug-disease (CtD, CpD) associations
    drug_gene_df = edges_df.filter(col("metaedge").isin("CuG", "CdG", "CbG"))
    drug_disease_df = edges_df.filter(col("metaedge").isin("CtD", "CpD"))
    
    # Count distinct genes associated with each compound (drug)
    # "source" represents the drug (compound), and "target" represents the gene
    drug_gene_count = drug_gene_df.groupBy("source").agg(countDistinct("target").alias("gene_count"))
    
    # Count distinct diseases associated with each compound (drug)
    # "source" represents the drug (compound), and "target" represents the disease
    drug_disease_count = drug_disease_df.groupBy("source").agg(countDistinct("target").alias("disease_count"))
    
    # Outer join the counts for each drug
    drug_counts = drug_gene_count.join(drug_disease_count, on="source", how="outer").fillna(0)
    
    # Sort by gene count and show the top 5 drugs with the highest number of genes
    drug_counts.orderBy(desc("gene_count")).show(5)

def question2():
    # Load edges data
    edges_df = spark.read.option("delimiter", "\t").option("header", True).csv(edges_path)

    # Step 1: Filter edges for drug-disease relationships (CtD, CpD)
    drug_disease_df = edges_df.filter(col("metaedge").isin("CtD", "CpD"))

    # Step 2: Count how many distinct drugs are associated with each disease
    disease_drug_count_df = drug_disease_df.groupBy("target").agg(countDistinct("source").alias("drug_count"))

    # Step 3: Count how many diseases are associated with each distinct drug count
    disease_count_by_drug_count_df = disease_drug_count_df.groupBy("drug_count").count().alias("disease_count")

    # Step 4: Sort by the number of diseases (descending) and select the top 5
    top_disease_counts_df = disease_count_by_drug_count_df.orderBy(col("count").desc()).limit(5)

    # Collect results to display in custom format
    results = top_disease_counts_df.collect()

    # Display the results in the desired format
    print("Q2: Top 5 count of diseases")
    for row in results:
        print(f"{row['drug_count']} drugs -> {row['count']} diseases")

def question3():
    # Load nodes and edges data
    nodes_df = spark.read.option("delimiter", "\t").option("header", True).csv(nodes_path)
    edges_df = spark.read.option("delimiter", "\t").option("header", True).csv(edges_path)

    # Step 1: Filter edges for drug-gene associations (CuG, CdG, CbG)
    drug_gene_df = edges_df.filter(col("metaedge").isin("CuG", "CdG", "CbG"))

    # Step 2: Count distinct genes associated with each drug
    # "source" represents the drug (compound), and "target" represents the gene
    drug_gene_count = drug_gene_df.groupBy("source").agg(countDistinct("target").alias("gene_count"))

    # Step 3: Ensure "gene_count" is treated as a numeric column for correct sorting
    drug_gene_count = drug_gene_count.withColumn("gene_count", col("gene_count").cast("int"))

    # Step 4: Join with nodes to get the drug names (assuming "source" in edges matches "id" in nodes)
    drug_gene_names = drug_gene_count.join(nodes_df, drug_gene_count.source == nodes_df.id, "inner") \
        .select(nodes_df.name.alias("drug_name"), "gene_count")  # Select drug names and gene count

    # Step 5: Sort by gene count in descending order
    top_drugs = drug_gene_names.orderBy(desc("gene_count")).limit(5)

    # Step 6: Collect results and display them in custom format
    results = top_drugs.collect()

    # Step 7: Print results in the desired format
    print("Drug Name -> Gene Count")
    for row in results:
        print(f"{row['drug_name']} -> {row['gene_count']}")

def main():
    #define parser
    parser = argparse.ArgumentParser(
        description="type of question"
    )
    #create arguments
    parser.add_argument(
        "-q","--question",metavar="question",
        choices=["question1","question2","question3"]
    )
    args = parser.parse_args()
    if args.question == "question1":
        question1()
    elif args.question == "question2":
        question2()
    elif args.question == "question3":
        question3()

if __name__ == "__main__":
    main()
    
    
