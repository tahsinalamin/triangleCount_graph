# triangleCount_graph
Counting triangles from a graph data set in vertica+python and spark ans shows the time.

******triangle_count.py******
counts the number of triangles from a graph stored in a DBMS with host python program. DBMS used is vertica. To run the program use the command- "python3 triangle_count.py dataset=xyz.csv"


******TriangleApp.scala******
counts the number of triangles from a graph stored as .txt file in hadoop. Load the graph(i j) in the HDFS and change the file directory. build.sbt is provided to compile the program. 
