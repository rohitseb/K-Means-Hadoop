#!/usr/bin/ksh
INDIR=input_dir
if [ -d "$INDIR" ]; then
    rm -rf "input_dir"
fi
SHAREDIR=shared_dir
if [ -d "$SHAREDIR" ]; then
    rm -rf "shared_dir"
fi
OUTDIR=output_dir
if [ -d "$OUTDIR" ]; then
    rm -rf "output_dir"
fi
KMeans=KMeans
if [ -d "$KMeans" ]; then
    rm -rf "KMeans"
fi
mkdir KMeans

Jar=KMeans.jar
if [ -f "$Jar" ] ; then
	rm KMeans.jar
fi

OP=finalOutput.txt
if [ -f "$OP" ] ; then
	rm finalOutput.txt
fi

read -p "Enter Number of Clusters  : " clusNum
read -p "Enter Cluster Centroids Seperated By Comma (Enter \"-\" to Randomly Assign Centroids  : " centroids
read -p "Enter Number of Iterations (Enter -1 to run till convergence)  : " iterations


javac -d KMeans KMeans.java 
jar -cvf KMeans.jar -C KMeans/ . 
/usr/local/hadoop/bin/hadoop fs -mkdir input_dir 
/usr/local/hadoop/bin/hadoop fs -put new_dataset_1.txt input_dir
/usr/local/hadoop/bin/hadoop fs -mkdir shared_dir
/usr/local/hadoop/bin/hadoop jar KMeans.jar hadoop.KMeans input_dir/new_dataset_1.txt output_dir shared_dir $clusNum $centroids $iterations
