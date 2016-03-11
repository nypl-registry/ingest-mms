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
  var insert = function (containers, callback) {
    // ask for a new bulk operation
    db.newRegistryIngestBulkOp('mmsContainers', (bulk) => {
      // insert all the operations
      containers.forEach((a) => bulk.insert(a))
      bulk.execute(function (err, result) {
        if (err) {
          console.log(err)
        }
        callback()
      })
    })
  }

  db.prepareRegistryIngestMmsContainers(function () {
    var filename = `${process.cwd()}${lexicon.configs.dataSourceFiles.mmsContainers}`

    exec(`wc -l ${filename}`, function (error, results) {
      if (error) console.log(error)

      var totalLines = results.trim().split(' ')[0]
      var totalInserted = 0

      var previousPercent = -1
      // if that did not work just set it to zero
      if (isNaN(totalLines)) totalLines = 0
      totalLines = parseInt(totalLines)

      db.returnCollectionRegistry('mmsContainers', function (err, container) {
        if (err) console.log(err)

        _(fs.createReadStream(filename))
          .split()
          .compact()
          .map(JSON.parse)
          .map((container) => {
            // build the base record off the idents
            var insert = mmsUtils.extractIds(container)

            insert._id = insert.mmsUuid

            // gather all the infos

            var xml = mmsUtils.returnXmlNode(container.full_xml)
            insert.rightsAgents = mmsUtils.extractMmsHashRightsAgents(container)

            if (xml) {
              insert.agents = mmsUtils.extractAgents(xml)
              insert.subjects = mmsUtils.extractSubjects(xml)
              insert.divisions = mmsUtils.extractDivision(xml)
              insert.notes = mmsUtils.extractNotes(xml)
              insert.titles = mmsUtils.extractTitles(xml)
              insert.languages = mmsUtils.extractLanguage(xml)
              insert.dates = mmsUtils.extractDates(xml)
              insert.abstracts = mmsUtils.extractAbstracts(xml)
              insert.typeOfResource = mmsUtils.extractTypeOfResource(xml)
              insert.genres = mmsUtils.extractGenres(xml)
              insert.physicalDescriptions = mmsUtils.extractPhysicalDescription(xml)
              insert.originInfos = mmsUtils.extractOriginInfo(xml)

              // get some hieararchy about it
              var h = mmsUtils.extractCollectionAndContainer(xml)

              insert.collectionUuid = h.collection
              insert.containerUuid = h.container
              insert.parents = h.parents
            } else {
              db.logError('MMS container ingest - no/invalid XML for this container ', insert._id)
              return ''
            }

            if (!insert.collectionUuid) {
              db.logError('MMS container ingest - no collection uuid found for this container ', insert._id)
              console.log(container.full_xml)
              return ''
            }

            var percent = Math.floor(++totalInserted / totalLines * 100)
            if (percent > previousPercent) {
              previousPercent = percent
              process.stdout.cursorTo(20)
              process.stdout.write(clc.black.bgMagentaBright('Container: ' + percent + '%'))
            }

            return insert
          })
          .compact()
          .batch(999)
          .map(_.curry(insert))
          .nfcall([])
          .series()
          .done(function () {
            console.log('Container Done')
            process.exit(0)
          })
      })
    })
  })
}
