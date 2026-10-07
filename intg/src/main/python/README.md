# Apache Atlas Python Client

`apache-atlas` is the official Python client package for Apache Atlas.
It provides typed helpers for entity, type definition, discovery, glossary,
lineage, relationship, and admin APIs.

## Requirements

- Python 3.13 or later
- Apache Atlas server reachable from your Python process

## Installation

Install the client from PyPI:

```bash
pip install apache-atlas
```

For Kerberos authentication, install the optional Kerberos dependency:

```bash
pip install requests-kerberos
```

Verify the installed package:

```bash
python -m pip show apache-atlas
```

## Supported Clients

`AtlasClient` is the main entry point. It exposes the following API clients:

- `entity`: create, read, update, delete entities and classifications
- `typedef`: manage type definitions
- `discovery`: search and full-text queries
- `glossary`: glossary terms and categories
- `lineage`: lineage graph queries
- `relationship`: relationship CRUD
- `admin`: server admin operations

## Quick Start

```python
from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import AtlasEntity, AtlasEntityWithExtInfo

client = AtlasClient('http://localhost:21000', ('admin', 'atlasR0cks!'))

test_db            = AtlasEntity({'typeName': 'hive_db'})
test_db.attributes = {'name': 'test_db', 'clusterName': 'prod', 'qualifiedName': 'test_db@prod'}

entity_info        = AtlasEntityWithExtInfo()
entity_info.entity = test_db

resp = client.entity.create_entity(entity_info)
guid = resp.get_assigned_guid(test_db.guid)
print(f'created test_db: guid={guid}')
```

## Authentication

Use the authentication mechanism configured for your Atlas deployment:

- Basic auth: pass a `(username, password)` tuple to `AtlasClient`.
- Kerberos/SPNEGO: pass `requests_kerberos.HTTPKerberosAuth()` after installing
  `requests-kerberos`.
- To disable SSL certificate validation (not recommended for production):
  `client.session.verify = False`

Example Kerberos setup:

```python
from requests_kerberos import HTTPKerberosAuth
from apache_atlas.client.base_client import AtlasClient

client = AtlasClient('http://localhost:21000', HTTPKerberosAuth())
```

## Examples and Code References

Runnable examples and additional usage patterns:

- Sample app:
  [`atlas-examples/sample-app/src/main/python/sample_client.py`](https://github.com/apache/atlas/blob/master/atlas-examples/sample-app/src/main/python/sample_client.py)
- Entity, lineage, glossary, discovery, and typedef examples in the same directory
- Unit tests:
  [`intg/src/test/python/test_atlas_client.py`](https://github.com/apache/atlas/blob/master/intg/src/test/python/test_atlas_client.py)

Run unit tests from the `intg/` module:

```bash
PYTHONPATH=src/main/python python -B src/test/python/test_atlas_client.py
```

## Version 0.0.16 Highlights

- Requires Python 3.13 or later (`python_requires='>=3.13'`)
- Updated `requests` dependency floor to `>=2.34.2`
- Expanded unit test coverage for client and model coercion

**Breaking change:** version 0.0.16 and later require Python 3.13+. Use 0.0.15
if you need an older Python runtime.
