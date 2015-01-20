--Message:
--        uuid
--        payload - HSET "data" -> uuid (first byte is version, the remainder is payload
--        expiration - ZSET "expirations:<channel>" -> (time) / uuid (optional)
--        deliveryCount - HSET "deliveries" -> uuid (int)
--        timestamp - HSET "timestamps" -> uuid
--        headers - HSET "headers" -> uuid (JSON)
--          type - json, stream
--          replyTo - channel to reply
-- send keys: channel:counter channel:active, channel:pending, data, channel:expirations, deliveries, timestamps, headers
-- KEYS: channel:reserved channel:pending data channel:expirations, deliveries, timestamps, headers
-- ARGS: uuid releaseAt

-- Move message to pending
-- Return headers and data

local reserved = KEYS[1]
local pending = KEYS[2]
local dataKey = KEYS[3]
local expirations = KEYS[4]
local deliveries = KEYS[5]
local timestamps = KEYS[6]
local headersKey = KEYS[7]

local uuid = ARGV[1]
local releaseAt = ARGV[2]

if tonumber(redis.call('LREM', reserved, 0, uuid)) < 1 and redis.call('ZSCORE', pending, uuid) == nil then return nil end
redis.call('ZADD', pending, releaseAt, uuid)
return {'headers', redis.call('HGET', headersKey, uuid), 
        'data', redis.call('HGET', dataKey, uuid), 
        'expiration', redis.call('ZSCORE', expirations, uuid),
        'deliveries', redis.call('HINCRBY', deliveries, uuid, 1),
        'timestamp', redis.call('HGET', timestamps, uuid)}