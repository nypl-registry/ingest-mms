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

  /**
   * Prune - pruneIgnoreCollections goes through the list of collection uuids and removes the blacklisted ones
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneIgnoreCollections = require(`${__dirname}/lib/prune`).pruneIgnoreCollections

  /**
   * Prune - pruneItemsMarkedAsExternal and remove external collections
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneItemsMarkedAsExternal = require(`${__dirname}/lib/prune`).pruneItemsMarkedAsExternal

  /**
   * Prune - pruneItemsWithInvalidContainer removes items that have a container uuid but it is not valid
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneItemsWithInvalidContainer = require(`${__dirname}/lib/prune`).pruneItemsWithInvalidContainer

  /**
   * Prune - pruneContainersWithoutCaptures removes items and containers that do not have any captures
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneContainersWithoutCaptures = require(`${__dirname}/lib/prune`).pruneContainersWithoutCaptures
  /**
   * Prune - pruneCollectionsWithoutCaptures removes collections (items + containers) that do not have any captures
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneCollectionsWithoutCaptures = require(`${__dirname}/lib/prune`).pruneCollectionsWithoutCaptures
  /**
   * Prune - pruneCollectionsWithoutCaptures removes items that do not have any captures
   *
   * @param  {function} cb - Nothing returned
   */
  this.pruneItemsWithoutCaptures = require(`${__dirname}/lib/prune`).pruneItemsWithoutCaptures
}

module.exports = exports = new IngestMms()
