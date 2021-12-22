# randgraph
an novel graph processing system for random walk

If you want to run in your platform, follow the two steps:
- `preprocess`, the `preprocess` procedure will process the row graph data, and output the binary csr format. The `preprocess` procedure will generate the following files:
    1. `.meta` contains the number of graph vertices and edges
    2. `.vert.blocks` contains the vertex block spliting point
    3. `.edge.blocks` contains the edge block spliting point
    4. `.beg`, `.csr`, `.wht` are the csr binary format, where `.wht` is the edge weight
    5. `.acc` contains the edge weight accumulate array
    6.  `.pb`, `.as` contains the vertex first-order transition probability array and alias array

The `preprocess` commnd
```bash
./bin/test/preprocess /home/hsc/dataset/livejournal/w-soc-livejournal.txt
```
- `run`, the `run` procedure will load some blocks into main memory, then perform second-order random walk on them.

The `run` commnd
```bash
./bin/test/walk /home/hsc/dataset/livejournal/w-soc-livejournal.txt
```