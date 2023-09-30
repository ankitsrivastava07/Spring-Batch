package com.batch.batchConfig;

import com.batch.dao.UserEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class CustomSkipListener implements SkipListener<UserEntity, UserEntity> {

    Logger logger = LoggerFactory.getLogger("");

    @Override
    public void onSkipInRead(Throwable t) {
        logger.error("Skip character is ", t);
    }

    @Override
    public void onSkipInWrite(UserEntity item, Throwable t) {
        logger.info("Skip character is ", item.getId());
    }

    @Override
    public void onSkipInProcess(UserEntity item, Throwable t) {
        logger.info("Skip character is ", item.getId());
    }
}
