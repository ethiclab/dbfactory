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
    if (typeof url != 'string') {
      throw Error('invalid url', url)
    }

    const deleteOne = (arr, line) => {
      const index = line - 1
      if (index < 0 && line >= arr.length) {
        throw Error(`Invalid line number ${line}`)
      }
      arr.splice(index, 1)
      return arr
    }

    const peek = async (dir, collectionName, line) => {
      const index = line - 1
      if (index < 0 && line >= arr.length) {
        throw Error(`Invalid line number ${line}`)
      }
      const colfile = path.join(dir, dbname, collectionName)
      if (!fs.existsSync(colfile)) {
        return null
      }
      const stats = fs.statSync(colfile)
      if (stats.isFile()) {
        let arr = fs.readFileSync(colfile, 'utf8').split(os.EOL)
        if (arr.length > 0 && arr.reverse()[0].length == 0) {
          arr = arr.slice(0, -1)
        }
        return arr[line - 1]
      } else {
        throw Error("Invalid file type.")
      }
    }

    const readAll = (dir, collectionName) => {
      const colfile = path.join(dir, dbname, collectionName)
      if (!fs.existsSync(colfile)) {
        return []
      }
      const stats = fs.statSync(colfile)
      if (stats.isFile()) {
        let arr = fs.readFileSync(colfile, 'utf8').split(os.EOL)
        console.log(arr)
        if (arr.length > 0 && arr[arr.length - 1].length == 0) {
          arr.pop()
        }
        return arr
      } else {
        throw Error("Invalid file type.")
      }
    }

    const count = async (dir, collectionName) => {
      const colfile = path.join(dir, dbname, collectionName)
      if (!fs.existsSync(colfile)) {
        return 0
      }
      const stats = fs.statSync(colfile)
      if (stats.isFile()) {
        let arr = fs.readFileSync(colfile, 'utf8').split(os.EOL)
        if (arr.length > 0 && arr.reverse()[0].length == 0) {
          arr = arr.slice(0, -1)
        }
        return arr.length
      } else {
        throw Error("Invalid file type.")
      }
    }

    return {
      beginTransaction: async () => {
        console.log(`cloning ${url}`)
        const dir = mktemp.createDirSync(`${os.tmpdir()}/XXXXXXXX`)
        await git.clone({ fs, http, dir, url: url })
        return dir
      },
      close: async dir => {
        deltree(dir)
      },
      commit: async (dir, message) => {
        await git.commit({
          fs,
          dir,
          message: message || 'no message provided',
          author: {
            name: 'EthicLab Team',
            email: 'tech@ethiclab.it'
          }
        })
      },
      publish: async dir => {
        await git.push({
          fs,
          http,
          dir: dir,
          remote: 'origin',
          ref: 'main',
          onAuth: () => ({ username: process.env.GITHUB_TOKEN })
        })
      },
      collection: async collectionName => {
        return {
          count: async dir => {
            return await count(dir, collectionName)
          },
          deleteOne: async (dir, id) => {
            const oldContent = readAll(dir, collectionName)
            console.log('old content', `[${oldContent.join(os.EOL)}]`)
            const newContent = deleteOne(oldContent, id)
            console.log('new content', `[${newContent.join(os.EOL)}]`)
            const colfile = path.join(dir, dbname, collectionName)
            fs.writeFileSync(colfile, newContent.join(os.EOL).concat(os.EOL))
            await git.add({fs, dir, filepath: path.join(dbname, collectionName)})
            return null
          },
          drop: async dir => {
            const file = path.join(dir, dbname, collectionName)
            if (fs.existsSync(file)) {
              fs.unlinkSync(file)
            }
            await git.remove({fs, dir, filepath: path.join(dbname, collectionName)})
          },
          replaceOne: async (dir, line, obj) => {
            const index = line - 1
            if (index < 0 && line >= arr.length) {
              throw Error(`Invalid line number ${line}`)
            }
            const oldContent = readAll(dir, collectionName)
            console.log('old content', `[${oldContent.join(os.EOL)}]`)
            const newContent = oldContent
            newContent[index] = obj
            console.log('new content', `[${newContent.join(os.EOL)}]`)
            const colfile = path.join(dir, dbname, collectionName)
            fs.writeFileSync(colfile, newContent.join(os.EOL).concat(os.EOL))
            await git.add({fs, dir, filepath: path.join(dbname, collectionName)})
            return {
              _id: newContent.count,
              text: obj
            }
          },
          find: async dir => {
            const currentContent = readAll(dir, collectionName).join(os.EOL)
            console.log('current content', `[${currentContent}]`)
            return readAll(dir, collectionName)
          },
          findOne: async (dir, id) => {
            return await peek(dir, collectionName, id)
          },
          insertOne: async (dir, obj, options) => {
            const params = options || {
              insertLineBefore: 0
            }
            if (typeof obj != 'string') {
              throw Error("this driver supports single strings only")
            }
            const dbfile = path.join(dir, dbname)
            if (!fs.existsSync(dbfile)) {
              // create dir by default
              fs.mkdirSync(dbfile, { recursive: true})
            }
            const oldContent = readAll(dir, collectionName)
            console.log('old content', `[${oldContent.join(os.EOL)}]`)
            const newContent = oldContent
            let id
            if (params.insertLineBefore) {
              const line = params.insertLineBefore
              const index = line - 1
              if (index < 0 && line >= arr.length) {
                throw Error(`Invalid line number ${line}`)
              }
              newContent.splice(index, 0, obj)
              id = params.line
            } else {
              newContent.push(obj)
              id = newContent.count
            }
            console.log('new content', `[${newContent.join(os.EOL)}]`)
            const colfile = path.join(dir, dbname, collectionName)
            fs.writeFileSync(colfile, newContent.join(os.EOL).concat(os.EOL))
            await git.add({fs, dir, filepath: path.join(dbname, collectionName)})
            return {
              _id: id,
              text: obj
            }
          }
        }
      }
    }
  }
  module.exports = f
})()
