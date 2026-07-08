package com.example.batch;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.batch.autoconfigure.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;

import javax.sql.DataSource;

@ImportRuntimeHints({
        BatchApplication.AppResourceHints.class, //
        BatchApplication.BatchHints.class
})
@SpringBootApplication
public class BatchApplication {

    private final static Resource CSV = new ClassPathResource("customers.csv");

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }


    static class AppResourceHints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
            hints.resources().registerResource(CSV);
            hints.reflection().registerType(Customer.class, MemberCategory.values());
        }
    }

    static class BatchHints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(@NonNull RuntimeHints hints, @Nullable ClassLoader classLoader) {
            for (var c : new Class[]{
                    org.springframework.batch.core.repository.persistence.JobInstance.class,
                    org.springframework.batch.core.repository.persistence.ExecutionContext.class,
                    org.springframework.batch.core.repository.persistence.ExitStatus.class,
                    org.springframework.batch.core.repository.persistence.StepExecution.class,
                    org.springframework.batch.core.repository.persistence.JobExecution.class,
                    org.springframework.batch.core.repository.persistence.JobParameter.class,
            }) {
                hints.reflection().registerType(c, MemberCategory.values());
            }

            var prefix = "org/springframework/batch/core/";
            for (var r : new String[]{
                    "schema-mongodb", //
                    "schema-drop-mongodb"}) {
                for (var suffix : "jsonl,js".split(",")) {
                    var path = prefix + r + "." + suffix;
                    var resource = new ClassPathResource(path);
                    if (resource.exists()) {
                        hints.resources().registerResource(resource);
                    }
                }
            }
        }


    }


    //
    static final String STEP_1 = "reset";
    //
    static final String STEP_2 = "csvToDbEtl";
    static final String STEP_2_READER = "csvToDbEtlReader";
    static final String STEP_2_WRITER = "csvToDbEtlWriter";


    @Bean(STEP_1)
    Step resetStep(JobRepository repository,
                   JdbcClient db) {
        return new StepBuilder("reset", repository)
                .tasklet((_, _) -> {
                    db.sql("delete from customers").update();
                    return RepeatStatus.FINISHED;
                })//
                .build();
    }

    @Bean(STEP_2_READER)
    FlatFileItemReader<Customer> csvFileItemReader() {
        return new FlatFileItemReaderBuilder<Customer>()
                .name(STEP_2_READER + "-reader")
                .resource(CSV)
                .linesToSkip(1)
                .targetType(Customer.class)
                .delimited(spec -> spec
                        .delimiter(",") //
                        .names("id", "name", "email") //
                )
//                .fieldSetMapper(fieldSet -> new Customer(fieldSet.readInt("id"),
//                        fieldSet.readString("name"),
//                        fieldSet.readString("email"))
//                )
                .build();
    }

    @Bean(STEP_2_WRITER)
    JdbcBatchItemWriter<Customer> jdbcBatchItemWriter(DataSource db) {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .beanMapped()
                .dataSource(db)
                .sql("insert into customers (id, name, email) values (:id, :name, :email)")
                .build();
    }

    @Bean(STEP_2)
    Step csvToDbEtlStep(JobRepository repository,
                        @Qualifier(STEP_2_READER) ItemReader<Customer> reader,
                        @Qualifier(STEP_2_WRITER) ItemWriter<Customer> writer
    ) {
        return new StepBuilder("csvToDbEtl", repository)
                .<Customer, Customer>chunk(10)
                .reader(reader)
                .writer(writer)
                .faultTolerant()
                .skipLimit(10)
                .retryLimit(10)
                .build();
    }

    @Bean
    Job csvToDbEtlJob(JobRepository repository,
                      @Qualifier(STEP_1) Step s1,
                      @Qualifier(STEP_2) Step s2
    ) {
        return new JobBuilder(repository)
                .incrementer(new RunIdIncrementer())
                .start(s1)
                .next(s2)
                .build();
    }

    @EventListener
    void onApplicationEvent(JobExecutionEvent event) {
        IO.println("finished " + event.getJobExecution()
                .getJobInstance().getJobName() + " @ " +
                event.getJobExecution().getEndTime() + "!");
    }

}

record Customer(int id, String name, String email) {
}

// Job
// -0..N Steps
//  (ItemReader, ItemProcessor, ItemWriter) || Tasklet
// step 1
// step 2
