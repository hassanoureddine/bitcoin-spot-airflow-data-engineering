select sum(asks.size), timestamp from `btcspot.btcspot.asks` as asks group by timestamp;
