var lexicon = require('nypl-registry-utils-lexicon')
var db = require('nypl-registry-utils-database')
var _ = require('highland')
var clc = require('cli-color')

var exports = module.exports = {}
/**
 * Prune - pruneIgnoreCollections goes through the list of collection uuids and removes the blacklisted ones
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneIgnoreCollections = function (cb) {
  console.log('pruneIgnoreCollections')

  db.returnCollections({registryIngest: ['mmsItems', 'mmsContainers', 'mmsCollections']}, function (err, returnCollections) {
    if (err) console.log(err)

    var mmsCollections = returnCollections.registryIngest.mmsCollections
    var mmsContainers = returnCollections.registryIngest.mmsContainers
    var mmsItems = returnCollections.registryIngest.mmsItems

    // all the collection uuids to delete
    var uuids = lexicon.configs.mmsIgnoreCollections

    var deleteStuff = function () {
      if (!uuids[0]) {
        if (cb) cb()
        return true
      }

      mmsItems.remove({collectionUuid: uuids[0]}, function (err, result) {
        if (err) console.log(err)

        mmsContainers.remove({collectionUuid: uuids[0]}, function (err, result) {
          if (err) console.log(err)

          mmsCollections.remove({_id: uuids[0]}, function (err, result) {
            if (err) console.log(err)
            console.log(uuids[0])
            uuids.shift()
            deleteStuff()
          })
        })
      })
    }

    deleteStuff()
  })
}
/**
 * Prune - pruneItemsMarkedAsExternal and remove external collections
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneItemsMarkedAsExternal = function (cb) {
  console.log('pruneItemsMarkedAsExternal')

  var totalItemsDeleted = 0
  // we are going to use this later to delete items
  db.returnCollectionRegistry('mmsItems', function (err, items) {
    if (err) console.log(err)
    var cursor = items.find({ divisions: /external/i })
    _(cursor)
      .map(_.wrapCallback((item, callback) => {
        items.remove({ _id: item._id }, function (err, result) {
          if (err) console.log(err)
          process.stdout.clearLine()
          process.stdout.cursorTo(0)
          process.stdout.write(clc.black.bgYellowBright(item._id + ' ' + ++totalItemsDeleted + ' for being external.'))

          callback(null, item)
        })
      }))
      .sequence()
      .done(function (err) {
        if (err) console.log(err)
        console.log('Done pruneItemsMarkedAsExternal')
        if (cb) cb()
      })
  })
}

/**
 * Prune - pruneItemsWithInvalidContainer removes items that have a container uuid but it is not valid
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneItemsWithInvalidContainer = function (cb) {
  console.log('pruneItemsWithInvalidContainer')
  var totalItemsDeleted = 0
  db.returnCollections({registryIngest: ['mmsItems', 'mmsContainers', 'mmsCollections']}, function (err, returnCollections) {
    if (err) console.log(err)

    var mmsContainers = returnCollections.registryIngest.mmsContainers
    var mmsItems = returnCollections.registryIngest.mmsItems

    var cursor = mmsItems.find({})

    _(cursor)
      .map((item) => {
        return (!item.containerUuid) ? '' : item
      })
      .compact()
      .map(_.wrapCallback((item, callback) => {
        mmsContainers.find({ _id: item.containerUuid }).count(function (err, result) {
          if (err) {
            console.log(err)
          }

          if (result === 0) {
            mmsItems.remove({ _id: item._id }, function (err, result) {
              if (err) console.log(err)

              totalItemsDeleted++
              process.stdout.clearLine()
              process.stdout.cursorTo(0)
              process.stdout.write(clc.black.bgYellowBright(item._id + ' ' + ++totalItemsDeleted + ' for not having a real container.'))

              callback(null, item)
            })
          } else {
            callback(null, item)
          }
        })
      }))
      .sequence()
      .done(function (err) {
        if (err) console.log(err)
        console.log('Done pruneItemsWithInvalidContainer')
        if (cb) cb()
      })
  })
}

/**
 * Prune - pruneContainersWithoutCaptures removes containers that do not have children captures
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneContainersWithoutCaptures = function (cb) {
  console.log('pruneContainersWithoutCaptures')
  var totalItemsDeleted = 0
  var totalContainersDeleted = 0
  db.returnCollections({registryIngest: ['mmsItems', 'mmsContainers', 'mmsCollections', 'mmsCaptures']}, function (err, returnCollections) {
    if (err) console.log(err)

    var mmsContainers = returnCollections.registryIngest.mmsContainers
    var mmsItems = returnCollections.registryIngest.mmsItems
    // var mmsCollections = returnCollections.registryIngest.mmsCollections
    var mmsCaptures = returnCollections.registryIngest.mmsCaptures
    var cursor = mmsContainers.find({})

    _(cursor)
      .map(_.wrapCallback((container, callback) => {
        mmsContainers.find({$or: [{ parents: container._id }, { _id: container._id }]}, { mmsDb: 1 }).toArray(function (err, result) {
          if (err) {
            console.log(err)
          }
          var containerMmsDbs = []

          for (var x in result) {
            containerMmsDbs.push(parseInt(result[x].mmsDb))
          }

          mmsCaptures.find({ containerMmsDb: { $in: containerMmsDbs } }, { mmsDb: 1 }).count(function (err, captureCount) {
            if (err) {
              console.log(err)
            }

            if (captureCount === 0) {
              mmsItems.remove({ containerUuid: container._id }, function (err, result) {
                if (err) console.log(err)
                totalItemsDeleted = totalItemsDeleted + result.result.n
                mmsContainers.remove({ _id: container._id }, function (err, result) {
                  if (err) console.log(err)
                  totalContainersDeleted = totalContainersDeleted + result.result.n

                  process.stdout.clearLine()
                  process.stdout.cursorTo(0)
                  process.stdout.write(clc.black.bgYellowBright('Deleted: ' + totalItemsDeleted + ' Items ' + totalContainersDeleted + ' Containers.'))

                  callback(null, container)
                })
              })
            } else {
              callback(null, container)
            }
          })
        })
      }))
      .sequence()
      .done(function (err) {
        if (err) console.log(err)
        console.log('Done pruneContainersWithoutCaptures')
        if (cb) cb()
      })
  })
}

/**
 * Prune - pruneCollectionsWithoutCaptures removes collections that do not have children captures
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneCollectionsWithoutCaptures = function (cb) {
  console.log('pruneCollectionsWithoutCaptures')
  var totalItemsDeleted = 0
  var totalContainersDeleted = 0
  var totalCollectionsDeleted = 0
  db.returnCollections({registryIngest: ['mmsItems', 'mmsContainers', 'mmsCollections', 'mmsCaptures']}, function (err, returnCollections) {
    if (err) console.log(err)

    var mmsContainers = returnCollections.registryIngest.mmsContainers
    var mmsItems = returnCollections.registryIngest.mmsItems
    var mmsCollections = returnCollections.registryIngest.mmsCollections
    var mmsCaptures = returnCollections.registryIngest.mmsCaptures
    var cursor = mmsCollections.find({})

    _(cursor)
      .map(_.wrapCallback((collection, callback) => {
        mmsCaptures.find({ collectionMmsDb: parseInt(collection.mmsDb) }).count(function (err, captureCount) {
          if (err) {
            console.log(err)
          }

          if (captureCount === 0) {
            mmsItems.remove({ collectionUuid: collection._id }, function (err, result) {
              if (err) console.log(err)

              totalItemsDeleted = totalItemsDeleted + result.result.n

              mmsContainers.remove({ collectionUuid: collection._id }, function (err, result) {
                if (err) console.log(err)

                totalContainersDeleted = totalContainersDeleted + result.result.n

                mmsCollections.remove({ _id: collection._id }, function (err, result) {
                  if (err) console.log(err)

                  totalCollectionsDeleted = totalCollectionsDeleted + result.result.n

                  process.stdout.clearLine()
                  process.stdout.cursorTo(0)
                  process.stdout.write(clc.black.bgYellowBright('Deleted: ' + totalItemsDeleted + ' Items ' + totalContainersDeleted + ' Containers ' + totalCollectionsDeleted + ' Collections.'))

                  callback(null, collection)
                })
              })
            })
          } else {
            callback(null, collection)
          }
        })
      }))
      .sequence()
      .done(function (err) {
        if (err) console.log(err)
        console.log('Done pruneCollectionsWithoutCaptures')
        if (cb) cb()
      })
  })
}
/**
 * Prune - pruneItemsWithoutCaptures removes items that do not have children captures
 *
 * @param  {function} cb - Nothing returned
 */
exports.pruneItemsWithoutCaptures = function (cb) {
  console.log('pruneItemsWithoutCaptures')
  var totalItemsDeleted = 0
  db.returnCollections({registryIngest: ['mmsItems', 'mmsCaptures']}, function (err, returnCollections) {
    if (err) console.log(err)

    var mmsItems = returnCollections.registryIngest.mmsItems
    var mmsCaptures = returnCollections.registryIngest.mmsCaptures
    var cursor = mmsItems.find({})

    _(cursor)
      .map(_.wrapCallback((item, callback) => {
        mmsCaptures.find({ itemUuid: item._id }).count(function (err, captureCount) {
          if (err) {
            console.log(err)
          }

          if (captureCount === 0) {
            mmsItems.remove({ _id: item._id }, function (err, result) {
              if (err) console.log(err)
              totalItemsDeleted = totalItemsDeleted + result.result.n
              if (err) console.log(err)

              process.stdout.clearLine()
              process.stdout.cursorTo(0)
              process.stdout.write(clc.black.bgYellowBright('Deleted: ' + totalItemsDeleted + ' Items '))

              callback(null, item)
            })
          } else {
            callback(null, item)
          }
        })
      }))
      .sequence()
      .done(function (err) {
        if (err) console.log(err)
        console.log('Done pruneItemsWithoutCaptures')
        if (cb) cb()
      })
  })
}
