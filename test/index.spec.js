const dbfactory = require("../src/index");
const assert = require("assert");
(async () => {
  let db = await dbfactory('https://github.com/ethiclab/testrepo.git', 'sys')
  let collection = await db.collection('file.txt')

  let transactionId = await db.beginTransaction()
  console.log('transaction id', transactionId)
  try {
    const initialCollectionObjects = await collection.find(transactionId)
    assert.deepEqual(['first line'], initialCollectionObjects, "initial collection state is not as expected")
    const countAtBeginning = await collection.count(transactionId)
    const firstCreatedObject = await collection.insertOne(transactionId, 'new created line')
    const afterCreate = await collection.find(transactionId)
    assert.deepEqual(['first line', 'new created line'], afterCreate, "collection state is not as expected")
    const allCollectionObjects = await collection.find(transactionId)
    const countAllCollectionObjects = await collection.count(transactionId)
    assert.equal(allCollectionObjects.length, countAllCollectionObjects, `Counting collection objects ${countAllCollectionObjects} and read collection objects ${allCollectionObjects.length} do not return the same number of items.`)
    assert.equal(countAtBeginning + 1, countAllCollectionObjects, "Number of objects did not increment by 1 as expected.")  
    const removed = await collection.deleteOne(transactionId, 2)
    assert.strictEqual(null, removed, "Remove method should return null")
    const countAtEnd = await collection.count(transactionId)
    assert.equal(countAtBeginning, countAtEnd)
    await collection.insertOne(transactionId, 'second line')
    await collection.insertOne(transactionId, 'third line')
    await collection.replaceOne(transactionId, 3, 'third line bis')
    const finalCollectionObjects = await collection.find(transactionId)
    assert.deepEqual(['first line', 'second line', 'third line bis'], finalCollectionObjects, "final collection state is not as expected")
    await collection.replaceOne(transactionId, 3, 'third line')
    await collection.insertOne(transactionId, 'new second line', { insertLineBefore: 2 })
    const finalCollectionObjects2 = await collection.find(transactionId)
    assert.deepEqual(['first line', 'new second line', 'second line', 'third line'], finalCollectionObjects2, "final collection state is not as expected")
    await collection.deleteOne(transactionId, 2)
    await db.commit(transactionId)
    await db.publish(transactionId)
  } finally {
    await db.close(transactionId)
  }

  transactionId = await db.beginTransaction()
  console.log('transaction id', transactionId)
  try {
    const initialCollectionObjects = await collection.find(transactionId)
    assert.deepEqual(['first line', 'second line', 'third line'], initialCollectionObjects, "initial collection state is not as expected")
  } finally {
    await db.close(transactionId)
  }

  transactionId = await db.beginTransaction()
  console.log('transaction id', transactionId)
  try {
    const initialCollectionObjects = await collection.find(transactionId)
    assert.deepEqual(['first line', 'second line', 'third line'], initialCollectionObjects, "initial collection state is not as expected")
    await collection.deleteOne(transactionId, 2)
    const afterOneDeletion = await collection.find(transactionId)
    assert.deepEqual(['first line', 'third line'], afterOneDeletion, "collection state is not as expected after removing second line")
    await collection.deleteOne(transactionId, 2)
    const finalCollectionObjects = await collection.find(transactionId)
    assert.deepEqual(['first line'], finalCollectionObjects, "initial collection state is not as expected")
    await db.commit(transactionId)
    await db.publish(transactionId)
  } finally {
    await db.close(transactionId)
  }

  db = await dbfactory('https://github.com/ethiclab/testrepo.git', 'src/main/c')
  collection = await db.collection('helloworld.c')

  transactionId = await db.beginTransaction()
  console.log('transaction id', transactionId)
  try {
    let line = 1
    await collection.replaceOne(transactionId, line++, '#include <stdio.h>')
    await collection.replaceOne(transactionId, line++, '')
    await collection.replaceOne(transactionId, line++, 'int main(const int argc, const char *argv[]) {')
    await collection.replaceOne(transactionId, line++, '    fprintf(stderr, "Hello, world!\\n");')
    await collection.replaceOne(transactionId, line++, '    return 0;')
    await collection.replaceOne(transactionId, line++, '}')
    await db.commit(transactionId, "fixed again helloworld.c")
    await db.publish(transactionId)
  } finally {
    await db.close(transactionId)
  }

})()
