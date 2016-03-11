'use strict'

module.exports = function (callback) {
  var lexicon = require('nypl-registry-utils-lexicon')
  // var utils = require('nypl-registry-utils-normalize')
  var _ = require('highland')
  var fs = require('fs')
  var db = require('nypl-registry-utils-database')
  var exec = require('child_process').exec
  var mmsUtils = require('../lib/utils.js')
  var clc = require('cli-color')

  // the bulk insert function, it gets a unordered bulk operation from the db
  var insert = function (collections, callback) {
    // ask for a new bulk operation
    db.newRegistryIngestBulkOp('mmsCollections', (bulk) => {
      // insert all the operations
      collections.forEach((a) => bulk.insert(a))
      bulk.execute(function (err, result) {
        if (err) {
          console.log(err)
        }
        callback()
      })
    })
  }

  db.prepareRegistryIngestMmsCollections(function () {
    var filename = `${process.cwd()}${lexicon.configs.dataSourceFiles.mmsCollections}`

    exec(`wc -l ${filename}`, function (error, results) {
      if (error) console.log(error)

      var totalLines = results.trim().split(' ')[0]
      var totalInserted = 0

      var previousPercent = -1
      // if that did not work just set it to zero
      if (isNaN(totalLines)) totalLines = 0
      totalLines = parseInt(totalLines)

      db.returnCollectionRegistry('mmsCollections', function (err, collection) {
        if (err) console.log(err)

        _(fs.createReadStream(filename))
          .split()
          .compact()
          .map(JSON.parse)
          .map((collection) => {
            // build the base record off the idents
            var insert = mmsUtils.extractIds(collection)
            insert._id = insert.mmsUuid

            // gather all the infos
            var xml = mmsUtils.returnXmlNode(collection.full_xml)
            insert.rightsAgents = mmsUtils.extractMmsHashRightsAgents(collection)
            if (xml) {
              insert.agents = mmsUtils.extractAgents(xml)
              insert.subjects = mmsUtils.extractSubjects(xml)
              insert.divisions = mmsUtils.extractDivision(xml)
              insert.notes = mmsUtils.extractNotes(xml)
              insert.titles = mmsUtils.extractTitles(xml)
              insert.languages = mmsUtils.extractLanguage(xml)
              insert.typeOfResource = mmsUtils.extractTypeOfResource(xml)
              insert.dates = mmsUtils.extractDates(xml)
              insert.abstracts = mmsUtils.extractAbstracts(xml)
              insert.genres = mmsUtils.extractGenres(xml)
              insert.physicalDescriptions = mmsUtils.extractPhysicalDescription(xml)
              insert.originInfos = mmsUtils.extractOriginInfo(xml)
            } else {
              db.logError('MMS collection ingest - no/invalid XML for this collection ', insert._id)
            }

            var percent = Math.floor(++totalInserted / totalLines * 100)
            if (percent > previousPercent) {
              previousPercent = percent
              process.stdout.cursorTo(0)
              process.stdout.write(clc.black.bgYellowBright('MMS Collection: ' + percent + '%'))
            }

            return insert
          })
          .batch(100)
          .map(_.curry(insert))
          .nfcall([])
          .series()
          .done(function () {
            console.log('Done')
            process.exit(0)
          })
      })
    })
  })
}
