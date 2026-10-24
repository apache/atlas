---
name: Nutch
route: /HookNutch
menu: Documentation
submenu: Hooks
---
import  * as theme  from 'react-syntax-highlighter/dist/esm/styles/hljs';
import SyntaxHighlighter from 'react-syntax-highlighter';

# Apache Atlas integration for Apache Nutch

Atlas catalogs Nutch crawls ([ATLAS-5399](https://issues.apache.org/jira/browse/ATLAS-5399)). Index-job lineage is emitted by the Nutch Atlas IndexWriter ([NUTCH-3210](https://issues.apache.org/jira/browse/NUTCH-3210)).

## Model

Entity types (`serviceType`: nutch), loaded from `addons/models/7000-Nutch/7010-nutch_model.json`:

* `nutch_crawl` (DataSet) — `{crawlId}@{clusterName}`
* `nutch_seedlist` (Asset) — `{crawlId}.seeds@{clusterName}` (COMPOSITION 1:1 under crawl)
* `nutch_segment` (DataSet) — `{crawlId}.{segmentName}@{clusterName}`
* `nutch_domain` (Asset) — eTLD+1 / assigned domain, not TLD. Example: `lucene.apache.org` → `apache.org`. `{etldPlusOne}@{clusterName}`
* `nutch_host` (Asset) — global hostname `{hostname}@{clusterName}`
* `nutch_index_process` (Process) — created by Nutch IndexingJob, not this bridge
* Index sink — generic `DataSet` `{collectionOrIndexName}@{clusterName}`

`nutch_crawl_hosts` is an ASSOCIATION with relationship attributes: fetchedCount, unfetchedCount, indexedCount, lastFetchTime, lastIndexedTime, maxScore. When hosts come from Nutch HostDB, optional attributes are also set: dnsFailures, connectionFailures, goneCount, redirPermCount, redirTempCount, homepageUrl.

## Dual-writer rules

* This bridge may create/update crawl, seedlist, segment, domain, host, and crawl–host metrics.
* The Nutch IndexWriter may create `nutch_index_process` and the index `DataSet`, and may update indexedCount / lastIndexedTime on an existing crawl–host relationship. It must not create crawl/host/domain entities.

## Cardinality

Do not model crawled URLs as Atlas entities. The bridge imports hosts that have at least one FETCHED URL (CrawlDb) or `fetched + notModified > 0` (HostDB).

## Importing CrawlDb / HostDB metadata

Hosts are preferred from Nutch HostDB (`{crawlId}/hostdb/current`, written by `UpdateHostDb`) when that directory exists. HostDB is optional in Nutch; if it is absent, hosts are rolled up from CrawlDb URLs (`hostSource=crawldb`). Crawl `urlCount`, seedlist, and segments still come from CrawlDb, the seed directory, and the segments directory.

Pass `-H` / `--hostdb` to force a HostDB path. Otherwise the bridge looks for `{parent(crawldb)}/hostdb/current`.

<SyntaxHighlighter wrapLines={true} language="shell" style={theme.dark}>
{`Usage: <atlas package>/hook-bin/import-nutch.sh -c <crawlId> -d <crawldb> [-H <hostdb>] [-s <seedDir>] [-g <segmentsDir>]`}
</SyntaxHighlighter>

Configure `atlas.rest.address` and `atlas.cluster.name`. Authenticate with username/password or a token the same way as other Atlas bridges.

## Out of scope (v1)

* URL/page entity types or URL sample lists
* Nutch REST admin / JobManager types (removed in [NUTCH-3165](https://issues.apache.org/jira/browse/NUTCH-3165))
* Consuming `indexer-kafka` JSON on `ATLAS_HOOK`
* Inject/fetch/parse Process types
