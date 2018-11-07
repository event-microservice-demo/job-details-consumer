// Setup MongoDB connection
const MongoClient = require('mongodb').MongoClient

const user = encodeURIComponent('root')
const password = encodeURIComponent('example')
const authMechanism = 'DEFAULT'

const url = `mongodb://${user}:${password}@localhost:27017/?authMechanism=${authMechanism}`

const dbName = "testjobs"

const insertDocument = (collection, message) => {
    MongoClient.connect(url)
    .then((client) => {
      const db = client.db(dbName)
      const document = JSON.parse(message) 
      db.collection(collection).update({ urn: document.urn }, document, { upsert: true } )
        .then(() => {
          console.log("Upserted document")
          client.close()
        })
        .catch(err => {
          console.log(err)
          client.close()
        })     
    })
    .catch(err => {
      console.log("Connection Failed")
      console.log(err)
    }) 
}

// Setup Kafka connection
const kafka = require('kafka-node')
const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})
const jobsConsumer = new kafka.Consumer(client,
        [{ topic: 'jobs', offset: 0}], { autoCommit: false })

jobsConsumer.on('message', (message) => {
    console.log(message);
    insertDocument("jobs", message.value)
});

jobsConsumer.on('error', (err) => {
    console.log('Error:', err);
})

jobsConsumer.on('offsetOutOfRange', (err) => {
    console.log('offsetOutOfRange:', err);
})

const client2 = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})
const companiesConsumer = new kafka.Consumer(client2,
    [{ topic: 'companies', offset: 0}], { autoCommit: false })

companiesConsumer.on('message', (message) => {
    console.log(message);
    insertDocument("companies", message.value)
});

companiesConsumer.on('error', (err) => {
    console.log('Error:', err);
})

companiesConsumer.on('offsetOutOfRange', (err) => {
    console.log('offsetOutOfRange:', err);
})