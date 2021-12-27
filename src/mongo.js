'use strict';
(function() {
  const mongoose = require('mongoose')
  let f = async function(mongoUrl, dbname) {
  try {
    let db = await mongoose.createConnection(`${mongoUrl}/${dbname}`, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    })
    return db
  } catch (e) {
    throw Error("oops")
  }
}
  module.exports = f
})()
