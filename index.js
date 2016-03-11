'use strict'

function IngestMms () {
  /**
   * Ingest the MMS collections
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingestCollections = require(`${__dirname}/lib/collections`)
}

module.exports = exports = new IngestMms()
