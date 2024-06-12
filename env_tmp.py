import os
print("printing env vars")
print(os.environ['HOME'])


print(os.environ)

print(os.environ.get('DUCKDB_CLUSTER_ADDR'))