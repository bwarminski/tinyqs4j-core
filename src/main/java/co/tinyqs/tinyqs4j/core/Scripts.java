package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;

import co.tinyqs.tinyredis.ScriptSHAPair;

class Scripts
{
    /**
     * -- KEYS: channel:counter channel:active, channel:pending, data, channel:expirations, deliveries, timestamps, headers
     * -- ARGS: channelName now headers delay expires data
     */
    public static final ScriptSHAPair SEND;
    
    /**
     * -- KEYS: channel:reserved channel:pending data headers
     * -- ARGS: uuid now
     */
    public static final ScriptSHAPair RECEIVE;
    
    /**
     * -- KEYS: channel:reserved channel:pending channel:active
     * -- ARGS: uuid
     */
    public static final ScriptSHAPair RELEASE;
    
    /**
     * -- KEYS: channel:reserved, channel:active, channel:pending, data, channel:expirations, deliveries, timestamps, headers
     * -- ARGS: uuid
     */
    public static final ScriptSHAPair ACKNOWLEDGE;
    
    /**
     * -- KEYS: channel:reserved channel:pending channel:active channel:expirations
     * -- ARGS: now
     */
    public static final ScriptSHAPair TICK;
    
    private static ScriptSHAPair _readScript(String filename) throws IOException
    {
        try (Reader reader = new InputStreamReader(Preconditions.checkNotNull(Scripts.class.getResourceAsStream(filename)), "UTF-8"))
        {
            String script = CharStreams.toString(reader);
            String sha = Hashing.sha1().hashString(script, Charsets.UTF_8).toString();
            return new ScriptSHAPair(script, sha);
        }
    }
    static
    {
        try
        {
            SEND = _readScript("send.lua");
            RECEIVE = _readScript("receive.lua");
            RELEASE = _readScript("release.lua");    
            ACKNOWLEDGE = _readScript("acknowledge.lua");
            TICK = _readScript("tick.lua");
        }        
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public static final List<ScriptSHAPair> ALL = ImmutableList.of(SEND,RECEIVE, RELEASE, ACKNOWLEDGE, TICK);
}
