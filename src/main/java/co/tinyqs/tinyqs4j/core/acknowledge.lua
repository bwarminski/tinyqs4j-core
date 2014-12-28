--Message:
--        uuid
--        payload - HSET "data" -> uuid (first byte is version, the remainder is payload
--        expiration - ZSET "expirations:<channel>" -> (time) / uuid (optional)
--        deliveryCount - HSET "deliveries" -> uuid (int)
--        timestamp - HSET "timestamps" -> uuid
--        headers - HSET "headers" -> uuid (JSON)
--          type - json, stream
--          replyTo - channel to reply
-- KEYS: channel:reserved, channel:active, channel:pending, data, channel:expirations, deliveries, timestamps, headers
-- ARGS: uuid

local reserved = KEYS[1]
local active = KEYS[2]
local pending = KEYS[3]
local data = KEYS[4]
local expirations = KEYS[5]
local deliveries = KEYS[6]
local timestamps = KEYS[7]
local headers = KEYS[8]

local uuid = ARGV[1]

local removed = redis.call('ZREM', pending, uuid)
if removed == 0 then
    removed = redis.call('LREM', reserved, 0, uuid)
    if removed == 0 then
        removed = redis.call('LREM', active, 0, uuid)
    end
end
if removed == 1 then
    redis.call('HDEL', data, uuid)
    redis.call('HDEL', timestamps, uuid);
    redis.call('HDEL', headers, uuid);
    redis.call('ZREM', expirations, uuid);
    redis.call('HDEL', deliveries, uuid);
end
return removed