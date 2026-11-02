# OpenSearch ML connector examples (dev bootstrap)

These JSON files are **request bodies** for `POST /_plugins/_ml/connectors/_create`.
Use them with `init-opensearch-semantic-remote.sh`:

```bash
cp openai-embedding.example.json my-connector.json
# edit credentials and model parameters in my-connector.json

EMBEDDING_DIMENSION=1536 REMOTE_MODEL_NAME="OpenAI text-embedding-3-small" \
  CONNECTOR_JSON=config/connectors/my-connector.json \
  ./scripts/init-opensearch-semantic-remote.sh
```

For Bedrock, Cohere, SageMaker, or other providers, use the matching
[OpenSearch connector blueprint](https://docs.opensearch.org/latest/ml-commons-plugin/remote-models/connectors/)
and pass that JSON as `CONNECTOR_JSON`.
