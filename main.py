import json

import pandas as pd
from opensearchpy import OpenSearch

opensearch_client =OpenSearch(
    hosts = [{"host": "localhost", "port": 9200}],
    http_auth = ("admin", "admin"),
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
)
def create_bulk_opensearch(index_name, dataset):
    bulk_opensearch = ''
    for row in dataset:
        bulk_opensearch += '{"index": {"_index": "%s"}}\n' % index_name
        bulk_opensearch += json.dumps(row) + '\n'
    return bulk_opensearch

dataset = json.loads(pd.read_csv('NintendoGames.csv').to_json(orient='records'))
bulk_opensearch = create_bulk_opensearch('nintendo_games', dataset)
print(bulk_opensearch)

#print(opensearch_client.bulk(body=bulk_opensearch, index='nintendo_games'))

print(opensearch_client.search(index='nintendo_games', body={"query": {"match_all": {}}}))
