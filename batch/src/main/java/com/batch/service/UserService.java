package com.batch.service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class UserService {

    @Autowired
    Job job;

    @Autowired
    @Qualifier("myJobLauncher")
    private JobLauncher jobLauncher;

    @Async
    public Map<String, Object> importUsers() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, Object> response = new HashMap<>();
        response.put("msg", "Success");
        response.put("status", Boolean.TRUE);

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("StartAt", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(job, jobParameters);

        return response;
    }
}
