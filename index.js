const MongoClient = require('mongodb').MongoClient;

// Connection URL
const user = encodeURIComponent('root');
const password = encodeURIComponent('example');
const authMechanism = 'DEFAULT';

const url = `mongodb://${user}:${password}@localhost:27017/?authMechanism=${authMechanism}`;

// const url = 'mongodb://localhost:27017';
const dbName = "testjobs"

const kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'}),
    consumer = new Consumer(client,
        [{ topic: 'jobs', offset: 0}],
        {
            autoCommit: false
        }
    );

consumer.on('message', (message) => {
  console.log(message);
  MongoClient.connect(url)
    .then((client) => {
      const db = client.db(dbName)
      console.log("Connected and retrieved db")
      db.collection('jobs').insertOne(JSON.parse(message.value))
        .then(result => {
          console.log(result)
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
});

consumer.on('error', (err) => {
    console.log('Error:',err);
})

consumer.on('offsetOutOfRange', (err) => {
    console.log('offsetOutOfRange:',err);
})