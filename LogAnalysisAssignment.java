import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Ответы на вопросы, инварианты и доказательства находятся в README.

public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    public static final class LogFileLines {

        public interface LineIterator extends Iterator<String>, AutoCloseable {
            @Override
            void close();
        }

        private LogFileLines() {}

        public static LineIterator iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new LineIterator() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public void close() {
                        if (closed) {
                            return;
                        }

                        closed = true;

                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }

                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };

        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };

        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-" + fileId + ".log"
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];

                        String path =
                                PATHS[random.nextInt(PATHS.length)];

                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];

                        long responseBytes =
                                100L + random.nextInt(100_000);

                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                method + " "
                                        + path + " "
                                        + status + " "
                                        + responseBytes + " "
                                        + durationMillis
                                        + "\n"
                        );
                    }
                }
            }
        }
    }

    static final class DamagedLineException extends RuntimeException {
        DamagedLineException(String message) {
            super(message);
        }
    }

    interface StatsAccumulator {
        void add(LogEntry entry);

        Report report();

        Map<String, PathStatistics> perUrl();
    }

    static final class PathAccumulator {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDurationMillis;
        long maxDurationMillis;

        void addUnlocked(LogEntry entry) {
            requests++;

            if (entry.status() >= 200 && entry.status() <= 399) {
                successful++;
            } else {
                errors++;
            }

            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();

            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        synchronized void add(LogEntry entry) {
            addUnlocked(entry);
        }

        synchronized PathStatistics toStatistics() {
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDurationMillis,
                    maxDurationMillis
            );
        }
    }

    static final class GlobalSyncStats implements StatsAccumulator {
        private final Map<String, PathAccumulator> byPath = new HashMap<>();

        @Override
        public synchronized void add(LogEntry entry) {
            PathAccumulator accumulator = byPath.get(entry.path());

            if (accumulator == null) {
                accumulator = new PathAccumulator();
                byPath.put(entry.path(), accumulator);
            }

            accumulator.addUnlocked(entry);
        }

        @Override
        public synchronized Report report() {
            long totalRequests = 0;
            long successfulRequests = 0;
            long errorRequests = 0;
            long responseBytes = 0;
            long maxDurationMillis = 0;

            for (PathAccumulator accumulator : byPath.values()) {
                PathStatistics statistics = accumulator.toStatistics();

                totalRequests += statistics.requests();
                successfulRequests += statistics.successful();
                errorRequests += statistics.errors();
                responseBytes += statistics.responseBytes();

                if (statistics.maxDurationMillis() > maxDurationMillis) {
                    maxDurationMillis = statistics.maxDurationMillis();
                }
            }

            return new Report(
                    totalRequests,
                    successfulRequests,
                    errorRequests,
                    responseBytes,
                    maxDurationMillis
            );
        }

        @Override
        public synchronized Map<String, PathStatistics> perUrl() {
            HashMap<String, PathStatistics> result = new HashMap<>();

            for (Map.Entry<String, PathAccumulator> entry : byPath.entrySet()) {
                result.put(entry.getKey(), entry.getValue().toStatistics());
            }

            return result;
        }
    }

    static final class PerUrlStats implements StatsAccumulator {
        private final ConcurrentHashMap<String, PathAccumulator> byPath =
                new ConcurrentHashMap<>();

        @Override
        public void add(LogEntry entry) {
            PathAccumulator accumulator = byPath.computeIfAbsent(
                    entry.path(),
                    key -> new PathAccumulator()
            );

            accumulator.add(entry);
        }

        @Override
        public Report report() {
            long totalRequests = 0;
            long successfulRequests = 0;
            long errorRequests = 0;
            long responseBytes = 0;
            long maxDurationMillis = 0;

            for (Map.Entry<String, PathAccumulator> entry : byPath.entrySet()) {
                PathStatistics statistics = entry.getValue().toStatistics();

                totalRequests += statistics.requests();
                successfulRequests += statistics.successful();
                errorRequests += statistics.errors();
                responseBytes += statistics.responseBytes();

                if (statistics.maxDurationMillis() > maxDurationMillis) {
                    maxDurationMillis = statistics.maxDurationMillis();
                }
            }

            return new Report(
                    totalRequests,
                    successfulRequests,
                    errorRequests,
                    responseBytes,
                    maxDurationMillis
            );
        }

        @Override
        public Map<String, PathStatistics> perUrl() {
            HashMap<String, PathStatistics> result = new HashMap<>();

            for (Map.Entry<String, PathAccumulator> entry : byPath.entrySet()) {
                result.put(entry.getKey(), entry.getValue().toStatistics());
            }

            return result;
        }
    }

    static final class StopFlag {
        volatile boolean stopRequested;

        void requestStop() {
            stopRequested = true;
        }

        boolean isStopRequested() {
            return stopRequested;
        }
    }

    static final class AnalysisCounters {
        final AtomicInteger filesProcessed = new AtomicInteger();
        final AtomicLong linesRead = new AtomicLong();
        final AtomicLong validRecords = new AtomicLong();
        final AtomicLong damagedRecords = new AtomicLong();
        final AtomicInteger readErrors = new AtomicInteger();
        final AtomicInteger activeFiles = new AtomicInteger();
        final AtomicInteger maxActiveFiles = new AtomicInteger();
        final AtomicLong stoppedFiles = new AtomicLong();

        void updateMaxActive(int current) {
            while (true) {
                int oldMax = maxActiveFiles.get();

                if (current <= oldMax) {
                    return;
                }

                if (maxActiveFiles.compareAndSet(oldMax, current)) {
                    return;
                }
            }
        }
    }

    public record AnalysisResult(
            Report report,
            Map<String, PathStatistics> perUrl,
            int filesProcessed,
            long linesRead,
            long validRecords,
            long damagedRecords,
            int readErrors,
            int maxActiveFiles,
            int activeFilesAfter,
            boolean stopped,
            long stoppedFiles,
            List<Path> failedFiles
    ) {}

    static LogEntry parseLine(String line) {
        if (line == null) {
            throw new DamagedLineException("Line is null");
        }

        String[] fields = line.trim().split("\\s+");

        if (fields.length != 5) {
            throw new DamagedLineException(
                    "Expected 5 fields: " + line
            );
        }

        if (fields[0].isEmpty() || fields[1].isEmpty()) {
            throw new DamagedLineException(
                    "Empty method or path: " + line
            );
        }

        try {
            int status = Integer.parseInt(fields[2]);
            long responseBytes = Long.parseLong(fields[3]);
            long durationMillis = Long.parseLong(fields[4]);

            if (status < 200 || responseBytes < 0 || durationMillis < 0) {
                throw new DamagedLineException(
                        "Invalid numeric field: " + line
                );
            }

            return new LogEntry(
                    fields[0],
                    fields[1],
                    status,
                    responseBytes,
                    durationMillis
            );
        } catch (NumberFormatException exception) {
            throw new DamagedLineException(
                    "Cannot parse numeric fields: " + line
            );
        }
    }

    static void processFile(
            Path file,
            StatsAccumulator stats,
            AnalysisCounters counters,
            StopFlag stopFlag,
            List<Path> failedFiles
    ) {
        LogFileLines.LineIterator iterator = null;

        try {
            iterator = LogFileLines.iterator(file);

            while (iterator.hasNext()) {
                if (stopFlag.isStopRequested()) {
                    counters.stoppedFiles.incrementAndGet();
                    return;
                }

                String line = iterator.next();
                counters.linesRead.incrementAndGet();

                try {
                    LogEntry entry = parseLine(line);
                    stats.add(entry);
                    counters.validRecords.incrementAndGet();
                } catch (DamagedLineException exception) {
                    counters.damagedRecords.incrementAndGet();
                }
            }

            counters.filesProcessed.incrementAndGet();
        } catch (RuntimeException exception) {
            counters.readErrors.incrementAndGet();
            failedFiles.add(file);
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (RuntimeException exception) {
                    // Ошибка закрытия уже не должна ломать анализ.
                }
            }
        }
    }

    static AnalysisResult makeResult(
            StatsAccumulator stats,
            AnalysisCounters counters,
            boolean stopped,
            List<Path> failedFiles
    ) {
        return new AnalysisResult(
                stats.report(),
                stats.perUrl(),
                counters.filesProcessed.get(),
                counters.linesRead.get(),
                counters.validRecords.get(),
                counters.damagedRecords.get(),
                counters.readErrors.get(),
                counters.maxActiveFiles.get(),
                counters.activeFiles.get(),
                stopped,
                counters.stoppedFiles.get(),
                failedFiles
        );
    }

    static AnalysisResult analyzeSequential(
            List<Path> files,
            StatsAccumulator stats
    ) {
        AnalysisCounters counters = new AnalysisCounters();
        StopFlag neverStop = new StopFlag();
        ArrayList<Path> failedFiles = new ArrayList<>();

        for (Path file : files) {
            int active = counters.activeFiles.incrementAndGet();
            counters.updateMaxActive(active);

            processFile(
                    file,
                    stats,
                    counters,
                    neverStop,
                    failedFiles
            );

            counters.activeFiles.decrementAndGet();
        }

        return makeResult(stats, counters, false, failedFiles);
    }

    static AnalysisResult analyzeParallel(
            List<Path> files,
            int workerThreads,
            int fileLimit,
            StatsAccumulator stats,
            StopFlag stopFlag
    ) throws InterruptedException {
        if (workerThreads <= 0 || fileLimit <= 0) {
            throw new IllegalArgumentException(
                    "workerThreads and fileLimit must be positive"
            );
        }

        AnalysisCounters counters = new AnalysisCounters();
        Vector<Path> failedFiles = new Vector<>();
        CountDownLatch latch = new CountDownLatch(files.size());
        ExecutorService pool = Executors.newFixedThreadPool(workerThreads);
        Semaphore semaphore = new Semaphore(fileLimit);

        try {
            for (Path file : files) {
                pool.execute(() -> {
                    boolean acquired = false;
                    int active = 0;

                    try {
                        if (stopFlag.isStopRequested()) {
                            counters.stoppedFiles.incrementAndGet();
                            return;
                        }

                        semaphore.acquire();
                        acquired = true;

                        if (stopFlag.isStopRequested()) {
                            counters.stoppedFiles.incrementAndGet();
                            return;
                        }

                        active = counters.activeFiles.incrementAndGet();
                        counters.updateMaxActive(active);

                        processFile(
                                file,
                                stats,
                                counters,
                                stopFlag,
                                failedFiles
                        );
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    } finally {
                        if (active > 0) {
                            counters.activeFiles.decrementAndGet();
                        }

                        if (acquired) {
                            semaphore.release();
                        }

                        latch.countDown();
                    }
                });
            }

            latch.await();
        } finally {
            pool.shutdown();

            try {
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException exception) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        return makeResult(
                stats,
                counters,
                stopFlag.isStopRequested(),
                failedFiles
        );
    }

    static List<Path> listFiles(Path directory) throws IOException {
        ArrayList<Path> result = new ArrayList<>();

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                result.add(path);
            }
        }

        return result;
    }

    static void generateOneUrlFile(Path file, int lines) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8
        )) {
            for (int i = 0; i < lines; i++) {
                writer.write("GET /hot 200 100 1\n");
            }
        }
    }

    static void generateManyUrlFile(
            Path file,
            int lines,
            int urlCount
    ) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8
        )) {
            for (int i = 0; i < lines; i++) {
                writer.write(
                        "GET /path" + (i % urlCount) + " 200 100 1\n"
                );
            }
        }
    }

    static void generateDamagedFile(Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8
        )) {
            writer.write("GET /ok 200 100 1\n");
            writer.write("DAMAGED LINE WITHOUT ENOUGH FIELDS\n");
        }
    }

    static boolean sameReport(Report left, Report right) {
        return left.totalRequests() == right.totalRequests()
                && left.successfulRequests() == right.successfulRequests()
                && left.errorRequests() == right.errorRequests()
                && left.responseBytes() == right.responseBytes()
                && left.maxDurationMillis() == right.maxDurationMillis();
    }

    static void checkBasicInvariants(AnalysisResult result) {
        if (result.activeFilesAfter() != 0) {
            throw new AssertionError(
                    "activeFilesAfter is not zero: "
                            + result.activeFilesAfter()
            );
        }

        if (result.maxActiveFiles() < 0) {
            throw new AssertionError(
                    "maxActiveFiles is negative: "
                            + result.maxActiveFiles()
            );
        }

        Report report = result.report();

        if (report.totalRequests()
                != report.successfulRequests() + report.errorRequests()) {
            throw new AssertionError(
                    "Total requests != successful + errors"
            );
        }

        Map<String, PathStatistics> perUrl = result.perUrl();

        long totalRequestsFromPerUrl = 0;
        long totalDuration = 0;
        long maxDuration = 0;

        for (PathStatistics statistics : perUrl.values()) {
            if (statistics.requests()
                    != statistics.successful() + statistics.errors()) {
                throw new AssertionError(
                        "requests != successful + errors for URL"
                );
            }

            if (statistics.requests() > 0
                    && statistics.maxDurationMillis()
                    > statistics.totalDurationMillis()) {
                throw new AssertionError(
                        "maxDuration > totalDuration for URL"
                );
            }

            totalRequestsFromPerUrl += statistics.requests();
            totalDuration += statistics.totalDurationMillis();

            if (statistics.maxDurationMillis() > maxDuration) {
                maxDuration = statistics.maxDurationMillis();
            }
        }

        if (totalRequestsFromPerUrl != report.totalRequests()) {
            throw new AssertionError(
                    "Report total does not match per-URL sum"
            );
        }

        if (report.totalRequests() > 0 && maxDuration > totalDuration) {
            throw new AssertionError(
                    "Global maxDuration > global totalDuration"
            );
        }

        if (report.maxDurationMillis() != maxDuration) {
            throw new AssertionError(
                    "Report maxDuration does not match per-URL max"
            );
        }
    }

    static void printSummary(
            String label,
            AnalysisResult result,
            long startNanos,
            long finishNanos
    ) {
        long timeMillis = (finishNanos - startNanos) / 1_000_000;
        Report report = result.report();

        System.out.println(label);
        System.out.println(" timeMillis: " + timeMillis);
        System.out.println(
                " totalRequests: " + report.totalRequests()
                        + ", successful: " + report.successfulRequests()
                        + ", errors: " + report.errorRequests()
        );
        System.out.println(
                " filesProcessed: " + result.filesProcessed()
                        + ", linesRead: " + result.linesRead()
                        + ", valid: " + result.validRecords()
                        + ", damaged: " + result.damagedRecords()
                        + ", readErrors: " + result.readErrors()
        );
        System.out.println(
                " maxActiveFiles: " + result.maxActiveFiles()
                        + ", activeAfter: " + result.activeFilesAfter()
                        + ", stopped: " + result.stopped()
                        + ", stoppedFiles: " + result.stoppedFiles()
                        + ", failedFiles: " + result.failedFiles().size()
        );
        System.out.println();
    }

    static AnalysisResult runParallelAndPrint(
            String label,
            List<Path> files,
            int workerThreads,
            int fileLimit,
            boolean perUrl
    ) throws InterruptedException {
        StatsAccumulator stats;

        if (perUrl) {
            stats = new PerUrlStats();
        } else {
            stats = new GlobalSyncStats();
        }

        StopFlag stopFlag = new StopFlag();

        long start = System.nanoTime();
        AnalysisResult result = analyzeParallel(
                files,
                workerThreads,
                fileLimit,
                stats,
                stopFlag
        );
        long finish = System.nanoTime();

        printSummary(label, result, start, finish);
        checkBasicInvariants(result);

        return result;
    }

    static void demo() throws Exception {
        Path directory = Files.createTempDirectory("log-demo-");
        GenerateLogs.generate(directory, 8, 2000);
        List<Path> files = listFiles(directory);

        long start = System.nanoTime();
        AnalysisResult sequential = analyzeSequential(
                files,
                new GlobalSyncStats()
        );
        long finish = System.nanoTime();

        printSummary("Sequential baseline", sequential, start, finish);
        checkBasicInvariants(sequential);

        AnalysisResult workers1 = runParallelAndPrint(
                "Parallel workers=1 semaphore=4 global",
                files,
                1,
                4,
                false
        );

        if (!sameReport(sequential.report(), workers1.report())) {
            throw new AssertionError(
                    "Parallel workers=1 report differs from sequential"
            );
        }

        AnalysisResult semaphore1 = runParallelAndPrint(
                "Parallel workers=4 semaphore=1 global",
                files,
                4,
                1,
                false
        );

        if (!sameReport(sequential.report(), semaphore1.report())) {
            throw new AssertionError(
                    "Parallel semaphore=1 report differs from sequential"
            );
        }

        AnalysisResult globalStats = runParallelAndPrint(
                "Parallel workers=4 semaphore=4 global",
                files,
                4,
                4,
                false
        );

        if (!sameReport(sequential.report(), globalStats.report())) {
            throw new AssertionError(
                    "Parallel global report differs from sequential"
            );
        }

        AnalysisResult perUrlStats = runParallelAndPrint(
                "Parallel workers=4 semaphore=4 per-URL",
                files,
                4,
                4,
                true
        );

        if (!sameReport(sequential.report(), perUrlStats.report())) {
            throw new AssertionError(
                    "Parallel per-URL report differs from sequential"
            );
        }

        Path oneUrlDirectory = Files.createTempDirectory("log-one-url-");
        for (int i = 0; i < 4; i++) {
            generateOneUrlFile(
                    oneUrlDirectory.resolve("one-" + i + ".log"),
                    5000
            );
        }
        List<Path> oneUrlFiles = listFiles(oneUrlDirectory);

        AnalysisResult oneUrlGlobal = runParallelAndPrint(
                "One hot URL, global lock",
                oneUrlFiles,
                4,
                4,
                false
        );

        AnalysisResult oneUrlPerUrl = runParallelAndPrint(
                "One hot URL, per-URL lock",
                oneUrlFiles,
                4,
                4,
                true
        );

        if (!sameReport(oneUrlGlobal.report(), oneUrlPerUrl.report())) {
            throw new AssertionError(
                    "One-URL global and per-URL reports differ"
            );
        }

        Path manyUrlDirectory = Files.createTempDirectory("log-many-url-");
        for (int i = 0; i < 4; i++) {
            generateManyUrlFile(
                    manyUrlDirectory.resolve("many-" + i + ".log"),
                    5000,
                    100
            );
        }
        List<Path> manyUrlFiles = listFiles(manyUrlDirectory);

        AnalysisResult manyUrlGlobal = runParallelAndPrint(
                "Many URLs, global lock",
                manyUrlFiles,
                4,
                4,
                false
        );

        AnalysisResult manyUrlPerUrl = runParallelAndPrint(
                "Many URLs, per-URL lock",
                manyUrlFiles,
                4,
                4,
                true
        );

        if (!sameReport(manyUrlGlobal.report(), manyUrlPerUrl.report())) {
            throw new AssertionError(
                    "Many-URL global and per-URL reports differ"
            );
        }

        Path damagedDirectory = Files.createTempDirectory("log-damaged-");
        generateOneUrlFile(damagedDirectory.resolve("good.log"), 100);
        generateDamagedFile(damagedDirectory.resolve("damaged.log"));
        List<Path> damagedFiles = listFiles(damagedDirectory);

        AnalysisResult damagedResult = runParallelAndPrint(
                "Damaged file test",
                damagedFiles,
                4,
                4,
                true
        );

        if (damagedResult.validRecords() != 101) {
            throw new AssertionError(
                    "Expected 101 valid records, got "
                            + damagedResult.validRecords()
            );
        }

        if (damagedResult.damagedRecords() != 1) {
            throw new AssertionError(
                    "Expected 1 damaged record, got "
                            + damagedResult.damagedRecords()
            );
        }

        if (damagedResult.readErrors() != 0) {
            throw new AssertionError(
                    "Damaged line should not be counted as file read error"
            );
        }

        StopFlag stopNow = new StopFlag();
        stopNow.requestStop();

        start = System.nanoTime();
        AnalysisResult stopped = analyzeParallel(
                files,
                4,
                4,
                new GlobalSyncStats(),
                stopNow
        );
        finish = System.nanoTime();

        printSummary("Stopped before start", stopped, start, finish);
        checkBasicInvariants(stopped);

        if (!stopped.stopped()) {
            throw new AssertionError("Stop flag was not reflected");
        }

        if (stopped.stoppedFiles() != files.size()) {
            throw new AssertionError(
                    "Expected all files to be marked stopped"
            );
        }
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;

                LogFileLines.LineIterator iterator =
                        LogFileLines.iterator(path);

                try {
                    while (iterator.hasNext()) {
                        String line = iterator.next();
                        String[] fields = line.trim().split("\\s+");

                        if (fields.length != 5) {
                            throw new AssertionError(
                                    "Unexpected line: " + line
                            );
                        }

                        Integer.parseInt(fields[2]);
                        Long.parseLong(fields[3]);
                        Long.parseLong(fields[4]);

                        lines++;
                    }
                } finally {
                    iterator.close();
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );

            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;

            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );

            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length == 0 || args[0].equals("demo")) {
            demo();
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment demo"
        );
    }
}
