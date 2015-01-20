-- KEYS: channel:reserved channel:pending channel:active
-- ARGS: now

local reserved = KEYS[1]
local pending = KEYS[2]
local active = KEYS[3]

local now = tonumber(ARGV[1])

-- Default to 5 sec for now
local newTtl = now + 5000

local val = redis.call('RPOP', reserved)
while val do
    redis.call('ZADD', pending, newTtl, val);
    val = redis.call('RPOP', reserved)
end

for index, value in pairs(redis.call('ZREVRANGEBYSCORE', pending, now, '-inf')) do
    redis.call('RPUSH', active, value)
    redis.call('ZREM', pending, value)
end