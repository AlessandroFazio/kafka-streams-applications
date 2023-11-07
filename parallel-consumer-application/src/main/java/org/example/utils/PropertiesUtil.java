package org.example.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try(InputStream fileInputStream = new FileInputStream(fileName)) {
            props.load(fileInputStream);
        }
        return props;
    }
}
