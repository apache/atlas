# Atlas Python Client

This is a python library for Atlas. Users can integrate with Atlas using the python client.
Currently, compatible with Python 3.5+

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install python client for Atlas.

```bash
# After publishing apache-atlas use

> pip install apache-atlas
```

Verify if apache-atlas client is installed:
```bash
> pip list

Package      Version
------------ ---------
apache-atlas 0.0.1
```

## Usage

```python create_glossary.py```
```python
# create_glossary.py

from apache_atlas.base_client import AtlasClient
from apache_atlas.model.glossary import AtlasGlossary

client        = AtlasClient("http://localhost:31000", "admin", "admin123")
glossary      = AtlasGlossary(None, None, "Glossary_Test", "This is a test Glossary")
test_glossary = client.glossary.create_glossary(glossary)

print('Created Test Glossary with guid: ' + test_glossary.guid)
```
For more examples, checkout `sample-app` python  project in atlas-examples module.