package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Longs;

public class LUAResourceLoaderTest
{
    public void run() throws IOException
    {
        try (Reader reader = new InputStreamReader(Preconditions.checkNotNull(LUAResourceLoaderTest.class.getResourceAsStream("send.lua")), "UTF-8");)
        {
            System.out.println(Hashing.sha1().hashString(CharStreams.toString(reader), Charsets.UTF_8).toString());
            
        }
        
    }
    public static void main(String[] args) throws IOException
    {
        
        new LUAResourceLoaderTest().run();
    }

}
