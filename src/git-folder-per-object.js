'use strict';
(function() {
  const os = require('os')
  const mktemp = require('mktemp')
  const deltree = require("deltree")
  const path = require('path')
  const git = require('isomorphic-git')
  const http = require('isomorphic-git/http/node')
  const fs = require('fs')
  let f = async function(url, dbname) {
    const dir = mktemp.createDirSync(`${os.tmpdir()}/XXXXXXXX`)
    if (typeof url != 'string') {
      throw Error('invalid url', url)
    }
    await git.clone({ fs, http, dir, url: url })

    const commitAndPush = async (collectionName, message) => {
      const sha = await git.commit({
        fs,
        dir,
        message: message,
        author: {
          name: 'EthicLab Team',
          email: 'tech@ethiclab.it'
        }
      })
      let pushResult = await git.push({
        fs,
        http,
        dir: dir,
        remote: 'origin',
        ref: 'main',
        onAuth: () => ({ username: process.env.GITHUB_TOKEN })
      })
    }

    const count = async (collectionName) => {
      const coldir = path.join(dir, dbname, collectionName)
      if (!fs.existsSync(coldir)) {
        return null
      }
      const stats = fs.statSync(coldir)
      if (stats.isDirectory()) {
        const retval = {}
        return fs.readdirSync(coldir).length
      } else {
        throw Error("Invalid file type.")
      }
    }

    const deleteOne = async (collectionName, objId) => {
      const objdir = path.join(dir, dbname, collectionName, `${objId}`)
      if (!fs.existsSync(objdir)) {
        return null
      }
      const stats = fs.statSync(objdir)
      if (stats.isDirectory()) {
        deltree(objdir)
      } else {
        throw Error("Invalid file type.")
      }
      await git.remove({fs, dir, filepath: path.join(dbname, collectionName, `${objId}`)})
      await commitAndPush(collectionName, "deleted object")
    }

    const findOne = async (collectionName, obj) => {
      const objdir = path.join(dir, dbname, collectionName, obj._id)
      if (!fs.existsSync(objdir)) {
        return null
      }
      const stats = fs.statSync(objdir)
      if (stats.isDirectory()) {
        const retval = {}
        fs.readdirSync(objdir).forEach(x => {
          try {
            retval[x] = JSON.parse(fs.readFileSync(path.join(objdir, x), { encoding: 'utf8' }))
          } catch (e) {
            retval[x] = fs.readFileSync(path.join(objdir, x), { encoding: 'utf8' })
          }
        })
        return retval
      } else {
        throw Error("Invalid file type.")
      }
    }
    return {
      close: async () => {
        deltree(dir)
      },
      collection: async collectionName => {
        return {
          countDocuments: async unused => {
            return await count(collectionName)
          },
          deleteOne: async id => {
            return await deleteOne(collectionName, id)
          },
          drop: async () => {
            // TODO
            throw Error("Unsupported")
          },
          replaceOne: async (old, obj, optionsUpsertFalse) => {
            // TODO
            throw Error("Unsupported")
          },
          find: async unusedMongooseApiCompat => {
            return {
              toArray: async () => {
                const coldir = path.join(dir, dbname, collectionName)
                if (!fs.existsSync(coldir)) {
                  return []
                }
                const stats = fs.statSync(coldir)
                if (stats.isDirectory()) {
		  const retval = []
                  fs.readdirSync(coldir).sort((a, b) => parseInt(a) - parseInt(b)).forEach(async x => {
                    const obj = await findOne(collectionName, { _id: x })
                    retval.push(obj)
                  })
                  return retval
                } else {
                  throw Error("Invalid file type.")
                }
              }
            }
          },
          findOne: async obj => {
            return await findOne(collectionName, obj)
          },
          insertOne: async (obj, unusedMongooseApiCompat) => {
            const dbfile = path.join(dir, dbname, collectionName)
            if (!fs.existsSync(dbfile)) {
              // create dir by default
              fs.mkdirSync(dbfile, { recursive: true})
            }
            // only folders are allowed
            const stats = fs.statSync(dbfile)
            if (stats.isDirectory()) {
              const ids = fs.readdirSync(dbfile).filter(x => {
                const f = path.join(dbfile, x)
                const stats = fs.statSync(f)
                return stats.isDirectory()
              }).filter(x => Number.isInteger(parseInt(x)) && parseInt(x) > 0).map(x => parseInt(x))
              // generate id
              if (ids.length === 0) {
                ids.push(0)
              }
              const id = (Math.max(...ids) + 1).toString()
              obj._id = id
              // create folder for new object
              const objectFolder = path.join(dbfile, id)
              fs.mkdirSync(objectFolder)
              // create a file for each entry
              Object.entries(obj).forEach(x => {
                const filename = x[0]
                const value = x[1]
                const entryFile = path.join(objectFolder, filename)
                if (Array.isArray(value)) {
                  fs.writeFileSync(entryFile, JSON.stringify(value, null, 4))
                } else if (typeof value === 'object') {
                  fs.writeFileSync(entryFile, JSON.stringify(value, null, 4))
                } else {
                  fs.writeFileSync(entryFile, value)
                }
              })
              await git.add({fs, dir, filepath: path.join(dbname, collectionName, `${obj._id}`)})
              await commitAndPush(collectionName, "created object")
            } else {
              throw Error("Invalid file type.")
            }
            obj.insertedId = obj._id
            return obj
          }
        }
      }
    }
  }
  module.exports = f
})()
