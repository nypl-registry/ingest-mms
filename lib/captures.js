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
  var insert = function (captures, callback) {
    // ask for a new bulk operation
    db.newRegistryIngestBulkOp('mmsCaptures', (bulk) => {
      // insert all the operations
      captures.forEach((a) => bulk.insert(a))
      bulk.execute(function (err, result) {
        if (err) {
          console.log(err)
        }
        callback()
      })
    })
  }

  db.prepareRegistryIngestMmsCaptures(function () {
    var filename = `${process.cwd()}${lexicon.configs.dataSourceFiles.mmsCaptures}`

    console.log('Checking file length...')
    exec(`wc -l ${filename}`, function (error, results) {
      if (error) console.log(error)

      var totalLines = results.trim().split(' ')[0]
      var totalInserted = 0

      var previousPercent = -1
      // if that did not work just set it to zero
      if (isNaN(totalLines)) totalLines = 0
      totalLines = parseInt(totalLines)

      db.returnCollectionRegistry('mmsCaptures', function (err, capture) {
        if (err) console.log(err)

        _(fs.createReadStream(filename))
          .split()
          .compact()
          .map(JSON.parse)
          .map((capture) => {
            // build the base record off the idents
            var insert = mmsUtils.extractCapture(capture)

            var percent = Math.floor(++totalInserted / totalLines * 100)
            if (percent > previousPercent) {
              previousPercent = percent
              process.stdout.cursorTo(45)
              process.stdout.write(clc.black.bgGreenBright('Captures: ' + percent + '%'))
            }

            return (capture) ? insert : ''
          })
          .compact()
          .batch(999)
          .map(_.curry(insert))
          .nfcall([])
          .series()
          .done(function () {
            console.log('Captures Done')
            process.exit(0)
          })
      })
    })
  })
}
