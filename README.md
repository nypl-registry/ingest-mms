# ingest-mms

[![travis](https://travis-ci.org/nypl-registry/ingest-mms.svg)](https://travis-ci.org/nypl-registry/ingest-mms/)

MMS to native JSON ingest

This module converts the data exported from MMS into native registry JSON. It also has a series of methods that prune the data once ingested to remove resources that do not have captures and other problems. The test suite covers the XML->JSON transformation. These jobs could be ran manually but are called from the [MMS Ingest dispatch job](https://github.com/nypl-registry/dispatch/blob/master/jobs/ingest_mms.js)