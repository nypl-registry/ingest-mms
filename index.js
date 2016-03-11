'use strict'

function IngestMms () {
  /**
   * Ingest the MMS collections
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingestCollections = require(`${__dirname}/lib/collections`)

  /**
   * Ingest the MMS collections
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingestContainers = require(`${__dirname}/lib/containers`)

  /**
   * Ingest the MMS Items
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingestItems = require(`${__dirname}/lib/items`)

  /**
   * Ingest the MMS Captures
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingestCaptures = require(`${__dirname}/lib/captures`)
}

module.exports = exports = new IngestMms()
