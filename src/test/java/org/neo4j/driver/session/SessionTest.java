package org.neo4j.driver.session;

import org.neo4j.driver.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(10)
public class SessionTest {

    Driver driver;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SessionTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        driver = GraphDatabase.driver("bolt://localhost");
    }

    @TearDown
    public void cleanup() {
        driver.close();
    }

    @Benchmark
    public void readAndWrite(Blackhole blackhole) {

        runQuery(blackhole, AccessMode.WRITE, "CREATE (n:Jmh) RETURN n");
        runQuery(blackhole, AccessMode.READ, "RETURN 2");
    }

    @Benchmark
    public void write(Blackhole blackhole) {

        runQuery(blackhole, AccessMode.WRITE, "CREATE (n:Jmh) RETURN n");
    }

    @Benchmark
    public void read(Blackhole blackhole) {

        runQuery(blackhole, AccessMode.READ, "RETURN 1");
    }

    private void runQuery(Blackhole blackhole, AccessMode accessMode, String query) {
        SessionConfig sessionConfig;
        sessionConfig = SessionConfig.builder()
                .withDefaultAccessMode(accessMode)
                .build();
        try (Session session = driver.session(sessionConfig)) {
            TransactionConfig txConfig = TransactionConfig.builder().withTimeout(Duration.ofMillis(100)).build();
            Result statementResult = session.run(query);//, txConfig);
            blackhole.consume(statementResult.consume());
        }
    }

}