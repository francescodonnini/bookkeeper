package org.apache.bookkeeper.bookie;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

public class TemporaryFile {
    private TemporaryFile() {}

    public static Path create(String name, String permissions, byte[] bytes) throws IOException {
        Path path = create(name, permissions);
        try (FileWriter writer = new FileWriter(path.toFile())) {
            for (byte b : bytes) {
                writer.write(b);
            }
        }
        System.out.println("Created temp file: " + path);
        return path;
    }

    public static Path create(String name, boolean writable) throws IOException {
        return create(name, writable ? "rw-rw-r--" : "r--r--r--");
    }

    public static Path create(String name, String permissions) throws IOException {
        return Files.createTempFile(name, null, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions)));
    }
}
