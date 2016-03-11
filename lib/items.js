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
  var insert = function (items, callback) {
    // ask for a new bulk operation
    db.newRegistryIngestBulkOp('mmsItems', (bulk) => {
      // insert all the operations
      items.forEach((a) => bulk.insert(a))
      bulk.execute(function (err, result) {
        if (err) {
          console.log(err)
        }
        callback()
      })
    })
  }

  db.prepareRegistryIngestMmsItems(function () {
    var filename = `${process.cwd()}${lexicon.configs.dataSourceFiles.mmsItems}`

    console.log('Checking file length...')
    exec(`wc -l ${filename}`, function (error, results) {
      if (error) console.log(error)

      var totalLines = results.trim().split(' ')[0]
      var totalInserted = 0

      var previousPercent = -1
      // if that did not work just set it to zero
      if (isNaN(totalLines)) totalLines = 0
      totalLines = parseInt(totalLines)

      db.returnCollectionRegistry('mmsItems', function (err, item) {
        if (err) console.log(err)

        _(fs.createReadStream(filename))
          .split()
          .compact()
          .map(JSON.parse)
          .map((item) => {
            // build the base record off the idents

            var insert = mmsUtils.extractIds(item)

            insert._id = insert.mmsUuid

            // gather all the infos
            var xml = mmsUtils.returnXmlNode(item.full_xml)
            insert.rightsAgents = mmsUtils.extractMmsHashRightsAgents(item)

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

              // rights
              var xmlRights = mmsUtils.returnXmlNode(item.rights)
              insert.rights = mmsUtils.extractRights(xmlRights)

              insert.publicDomain = false
              insert.dcFlag = false

              if (insert.rights.join(' ').search('We believe that this item has no known US copyright restrictions') > -1) insert.publicDomain = true
              if (insert.rights.join(' ').search('Creative Commons CC0') > -1) insert.publicDomain = true
              if (insert.rights.join(' ').search('Can be used on NYPL website') > -1) insert.dcFlag = true

              var percent = Math.floor(++totalInserted / totalLines * 100)
              if (percent > previousPercent) {
                previousPercent = percent
                process.stdout.cursorTo(35)
                process.stdout.write(clc.black.bgCyanBright('Items: ' + percent + '% '))
              }

              return insert
            } else {
              db.logError('MMS items ingest - no/invalid XML for this item ', insert._id)
              return ''
            }
          })
          .compact()
          .batch(999)
          .map(_.curry(insert))
          .nfcall([])
          .series()
          .done(function () {
            console.log('Items Done')
            process.exit(0)
          })
      })
    })
  })
}
