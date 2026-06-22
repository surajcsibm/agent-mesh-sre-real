const {Kafka} = require('kafkajs');
const k = new Kafka({clientId:'test', brokers:['212.2.248.241:9092']});
const a = k.admin();
a.connect()
  .then(() => a.listTopics())
  .then(t => { console.log('Connected! Topics:', t.length); console.log(t.join('\n')); a.disconnect(); })
  .catch(e => console.error('FAILED:', e.message));
