package test.spark;

import static com.google.common.io.Resources.getResource;

import lombok.SneakyThrows;

public class Utils {
    @SneakyThrows
    public static String getResourceUrl(String path) {
        return getResource(path).toURI().getPath();
    }
}
