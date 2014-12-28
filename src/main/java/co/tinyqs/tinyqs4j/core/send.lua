--Message:
--        uuid
--        payload - HSET "data" -> uuid (first byte is version, the remainder is payload
--        expiration - ZSET "expirations:<channel>" -> (time) / uuid (optional)
--        deliveryCount - HSET "deliveries" -> uuid (int)
--        timestamp - HSET "timestamps" -> uuid
--        headers - HSET "headers" -> uuid (JSON)
--          type - json, stream
--          replyTo - channel to reply
-- KEYS: channel:counter channel:active, channel:pending, data, channel:expirations, deliveries, timestamps, headers
-- ARGS: channelName now  headers delay expires data

local counter = KEYS[1]
local active = KEYS[2]
local pending = KEYS[3]
local dataMap = KEYS[4]
local expirations = KEYS[5]
local deliveries = KEYS[6]
local timestamps = KEYS[7]
local headersKey = KEYS[8]

local channelName = ARGV[1]
local now = tonumber(ARGV[2])
local headersVal = ARGV[3]
local delay = tonumber(ARGV[4])
local expires = tonumber(ARGV[5])
local data = ARGV[6]

if expires > 0 and expires <= now then
  return 0
end
local uuid = channelName .. ':' .. redis.call('INCR', counter)
redis.call('HSET', dataMap, uuid, data)
redis.call('HSET', timestamps, uuid, now)
redis.call('HSET', headersKey, uuid, headersVal)
if expires >= 0 then
    redis.call('ZADD', expirations, expires, uuid)
end
if delay >= 0 and delay > now then
    redis.call('HSET', deliveries, uuid, -1)
    redis.call('ZADD', pending, delay, uuid)
else
    redis.call('HSET', deliveries, uuid, 0)
    redis.call('LPUSH', active, uuid)
end
return uuid