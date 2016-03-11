'use strict'

function IngestMms () {
  /**
   * Will prompt for the location of the FAST data
   *
   * @param  {function} cb - Nothing returned
   */
  // this.ingest = require(__dirname + '/lib/ingest')(this)

  var lexicon = require('nypl-registry-utils-lexicon')
  console.log(lexicon.maps)
}

module.exports = exports = new IngestMms()
