package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.log4j.BasicConfigurator;
import java.net.InetSocketAddress;

public class Main {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        try {
            CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("159.223.62.129", 9042))
                    .withLocalDatacenter("dc1")
                    .build();

            while(true) {
                ResultSet rs = session.execute("SELECT release_version FROM system.local");
                for (Row row : rs) {
                    System.out.println(row);
                }
            }

        } catch (Exception e){
            System.out.println(e);
        }
    }
}