package FlinkSql;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class FlinkSql04 {
    public static void main(String[] args) throws Exception {
        URLClassLoader c = new URLClassLoader(new URL[]{new URL("http://maven.mogujie.org/nexus/service/local/repositories/releases/content/com/mogujie/moguflink/moguflink-client/1.1.8.1/moguflink-client-1.1.8.1.jar")});

        Class aClass = c.loadClass("com.mogujie.flink.client.entity.TaskParams");

        String s = aClass.newInstance().toString();
        System.out.println(s);
    }
}
