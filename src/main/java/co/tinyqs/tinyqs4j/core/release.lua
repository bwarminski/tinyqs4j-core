-- KEYS: channel:reserved channel:pending channel:active
-- ARGS: uuid

local reserved = KEYS[1]
local pending = KEYS[2]
local active = KEYS[3]

local uuid = ARGV[1]

local removedIt = false
removedIt = tonumber(redis.call('ZREM', pending, uuid)) == 1
if not removedIt then
  removedIt = tonumber(redis.call('LREM', reserved, 0, uuid)) > 0
end
if removedIt then
  redis.call('LPUSH', active, uuid)
end
return removedIt
  