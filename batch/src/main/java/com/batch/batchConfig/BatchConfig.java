package com.batch.batchConfig;

import com.batch.dao.UserEntity;
import com.batch.dao.repository.UserRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private UserRepository userRepository;

    @Autowired
    DataSource dataSource;

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate;
    @Autowired
    JobRepository jobRepository;
    private final static String SQL_INSERT_QUERY = "insert into users(age,country,created_at,first_name,last_name,gender) values(:age,:country,:createdAt,:firstName,:lastName,:gender)";

    @Bean
    public FlatFileItemReader<UserEntity> itemReader() {
        FlatFileItemReader<UserEntity> flatFileItemReader = new FlatFileItemReader<>();

        flatFileItemReader.setName("Reading User Data from user_list.csv file");
        flatFileItemReader.setResource(new ClassPathResource("users_list.csv"));
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    private LineMapper<UserEntity> lineMapper() {
        DefaultLineMapper<UserEntity> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(",");
        delimitedLineTokenizer.setNames("First Name", "Last Name", "Gender", "Country", "Age", "Created At");
        // delimitedLineTokenizer.setStrict(true);

        //  delimitedLineTokenizer.setIncludedFields(0, 1, 2, 3, 4, 5, 6);

        BeanWrapperFieldSetMapper<UserEntity> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        beanWrapperFieldSetMapper.setTargetType(UserEntity.class);
        lineMapper.setLineTokenizer(delimitedLineTokenizer);
        lineMapper.setFieldSetMapper(new CustomUserEntityFieldSetMapper());

        return lineMapper;
    }

    @Bean
    public UserItemProcessor itemProcessor() {
        return new UserItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<UserEntity> itemWriter() {

        JdbcBatchItemWriter<UserEntity> jdbcBatchItemWriterBuilder = new JdbcBatchItemWriter<>();

        jdbcBatchItemWriterBuilder.setSql(SQL_INSERT_QUERY);
        jdbcBatchItemWriterBuilder.setDataSource(dataSource);
        jdbcBatchItemWriterBuilder.setJdbcTemplate(jdbcTemplate);
        jdbcBatchItemWriterBuilder.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<UserEntity>());

       /* RepositoryItemWriter<UserEntity> repositoryItemWriter = new RepositoryItemWriter<>();
        repositoryItemWriter.setMethodName("save");
        repositoryItemWriter.setRepository(userRepository);*/
        return jdbcBatchItemWriterBuilder;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("users_list.csv file")
                .<UserEntity, UserEntity>chunk(40000)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .faultTolerant()
                .skip(DataIntegrityViolationException.class)
                .skipLimit(3)
                .listener(new CustomSkipListener())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("import Users From File")
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new TaskExecutorBuilder()
                .maxPoolSize(100000)
                .corePoolSize(100000)
                .queueCapacity(100)
                .build();
    }

    @Bean(name = "myJobLauncher")
    public JobLauncher simpleJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}
