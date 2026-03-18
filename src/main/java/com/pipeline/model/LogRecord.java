package com.pipeline.model;

import java.util.Collections;
import java.util.List;

public class LogRecord {

    public static final LogRecord POISON_PILL = new LogRecord(
            "__POISON_PILL__", "POISON", -1, false, "", Collections.emptyList()
    );

    private final String rawLine;
    private final String level;
    private final long lineNumber;
    private final boolean isError;
    private final String timestamp;
    private final List<String> words;

    public LogRecord(String rawLine, String level, long lineNumber,
                     boolean isError, String timestamp, List<String> words) {
        this.rawLine    = rawLine;
        this.level      = level;
        this.lineNumber = lineNumber;
        this.isError    = isError;
        this.timestamp  = (timestamp != null) ? timestamp : "";
        this.words      = (words != null)
                ? Collections.unmodifiableList(words)
                : Collections.emptyList();
    }

    public LogRecord(String rawLine) {
        this(rawLine, "UNKNOWN", -1, false, "", Collections.emptyList());
    }

    public LogRecord(String rawLine, long lineNumber) {
        this(rawLine, "UNKNOWN", lineNumber, true, "", Collections.emptyList());
    }

    public boolean isPoisonPill() {
        return this == POISON_PILL;
    }

    public boolean isError() {
        return isError;
    }
    public String       getRawLine()    { return rawLine; }
    public String       getLevel()      { return level; }
    public long         getLineNumber() { return lineNumber; }
    public String       getTimestamp()  { return timestamp; }

    public List<String> getWords()      { return words; }

    @Override
    public String toString() {
        if (isPoisonPill()) return "[POISON_PILL]";
        return String.format(
                "LogRecord{line=%d, level='%s', error=%b, words=%d, ts='%s', raw='%s'}",
                lineNumber, level, isError, words.size(), timestamp,
                rawLine.length() > 60 ? rawLine.substring(0, 60) + "..." : rawLine
        );
    }
}