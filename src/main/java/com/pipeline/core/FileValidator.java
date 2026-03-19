package com.pipeline.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FileValidator {

    private static final Pattern LOG_LINE_PATTERN = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2})?\\s*(INFO|ERROR|WARN|DEBUG|TRACE)?.*",
            Pattern.CASE_INSENSITIVE
    );

    private static final int SAMPLE_SIZE = 10;

    private static final double MAX_BINARY_RATIO = 0.1;

    public static class ValidationResult {
        private final boolean valid;
        private final String  reason;
        private final long    fileSizeBytes;
        private final int     sampleLinesRead;

        ValidationResult(boolean valid, String reason, long fileSizeBytes, int sampleLinesRead) {
            this.valid           = valid;
            this.reason          = reason;
            this.fileSizeBytes   = fileSizeBytes;
            this.sampleLinesRead = sampleLinesRead;
        }

        public boolean isValid()          { return valid; }
        public String  getReason()        { return reason; }
        public long    getFileSizeBytes() { return fileSizeBytes; }
        public int     getSampleLines()   { return sampleLinesRead; }

        @Override
        public String toString() {
            return String.format(
                    "ValidationResult{valid=%b, reason='%s', fileSize=%d bytes, sampleLines=%d}",
                    valid, reason, fileSizeBytes, sampleLinesRead
            );
        }
    }

    public static ValidationResult validate(String filePath) {
        File file = new File(filePath);

        if (!file.exists()) {
            return fail("File does not exist: " + filePath, 0, 0);
        }
        if (!file.isFile()) {
            return fail("Path is not a regular file: " + filePath, 0, 0);
        }
        if (!file.canRead()) {
            return fail("File is not readable (check permissions): " + filePath, 0, 0);
        }

        long fileSize = file.length();
        if (fileSize == 0) {
            return fail("File is empty: " + filePath, fileSize, 0);
        }

        List<String> sampleLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while (sampleLines.size() < SAMPLE_SIZE && (line = reader.readLine()) != null) {
                sampleLines.add(line);
            }
        } catch (IOException e) {
            return fail("IOException while reading sample: " + e.getMessage(), fileSize, 0);
        }

        if (sampleLines.isEmpty()) {
            return fail("File has no readable lines: " + filePath, fileSize, 0);
        }

        for (String line : sampleLines) {
            if (isBinaryLine(line)) {
                return fail(
                        "File appears to be binary or non-text (null bytes or high binary ratio " +
                                "detected in first " + sampleLines.size() + " lines). " +
                                "Only UTF-8 text files are supported.",
                        fileSize, sampleLines.size()
                );
            }
        }

        boolean anyLineMatches = sampleLines.stream()
                .anyMatch(line -> LOG_LINE_PATTERN.matcher(line).matches());

        if (!anyLineMatches) {
            return fail(
                    "None of the first " + sampleLines.size() + " lines match the expected " +
                            "log/text format. Possible encrypted or unknown format file.",
                    fileSize, sampleLines.size()
            );
        }

        return new ValidationResult(true,
                "File is valid. Size=" + fileSize + " bytes, sample=" + sampleLines.size() + " lines.",
                fileSize, sampleLines.size()
        );
    }

    private static boolean isBinaryLine(String line) {
        if (line.isEmpty()) return false;
        int nonPrintable = 0;
        for (char c : line.toCharArray()) {
            if (c == '\0') return true;
            if (c < 32 && c != '\t') nonPrintable++;
        }
        return ((double) nonPrintable / line.length()) > MAX_BINARY_RATIO;
    }

    private static ValidationResult fail(String reason, long fileSize, int sampleLines) {
        return new ValidationResult(false, reason, fileSize, sampleLines);
    }
}