import ZooKeeper from './config/zookeeper';


(async () => {
  await ZooKeeper.connectZookeeper();
  await require('./config/index');
  await require('./server');
})();